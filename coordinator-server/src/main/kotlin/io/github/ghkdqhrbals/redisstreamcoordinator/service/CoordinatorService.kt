package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorException
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.protocol.CoordinatorCompatibilityResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.protocol.CoordinatorProtocol
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import io.github.ghkdqhrbals.redisstreamcoordinator.store.CoordinatorStateConflictException
import io.github.ghkdqhrbals.redisstreamcoordinator.store.CoordinatorStateStore
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.NoopStreamShardProvisioner
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.RedisStreamShardKeys
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.RedisStreamShardProvisioningPlan
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.StreamShardProvisioner
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.streamShardProvisioningPlan
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.streamShardKeys
import org.springframework.beans.factory.ObjectProvider
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

data class CoordinatorTickResult(
    val scannedGroups: Int,
    val changedGroups: Int,
)

private data class ShardRateKey(
    val streamPrefix: String,
    val consumerGroup: String,
    val shard: ShardId,
)

private data class ShardRateSnapshot(
    val observedAt: Instant,
    val streamLength: Long,
    val lag: Long?,
) {
    fun rateFrom(previous: ShardRateSnapshot?): ShardRate {
        if (previous == null) {
            return ShardRate(producedPerSecond = null, consumedPerSecond = null)
        }
        val seconds = Duration.between(previous.observedAt, observedAt).toMillis() / 1000.0
        if (seconds <= 0.0) {
            return ShardRate(producedPerSecond = null, consumedPerSecond = null)
        }
        val producedDelta = (streamLength - previous.streamLength).coerceAtLeast(0)
        val lagDelta = lag?.let { currentLag ->
            previous.lag?.let { previousLag -> currentLag - previousLag }
        }
        val consumedDelta = lagDelta?.let { (producedDelta - it).coerceAtLeast(0) }
        return ShardRate(
            producedPerSecond = (producedDelta / seconds).roundRate(),
            consumedPerSecond = consumedDelta?.let { (it / seconds).roundRate() },
        )
    }
}

private data class ShardRate(
    val producedPerSecond: Double?,
    val consumedPerSecond: Double?,
)

private data class MonitoringOffsetCacheKey(
    val streamPrefix: String,
    val consumerGroup: String,
)

private data class MonitoringOffsetCacheEntry(
    val shardCount: Int,
    val expiresAt: Instant,
    val offsets: StreamShardOffsetsResponse,
)

private data class RedisHealthCacheEntry(
    val expiresAt: Instant,
    val status: String,
)

private fun Double.roundRate(): Double =
    kotlin.math.round(this * 100.0) / 100.0

private fun Iterable<ShardRate>.sumKnownOrNull(selector: (ShardRate) -> Double?): Double? {
    var sum = 0.0
    var found = false
    forEach { rate ->
        val value = selector(rate)
        if (value != null) {
            sum += value
            found = true
        }
    }
    return if (found) sum.roundRate() else null
}

@Service
class CoordinatorService(
    private val properties: CoordinatorProperties,
    private val stateStore: CoordinatorStateStore,
    private val redisConnectionFactory: ObjectProvider<RedisConnectionFactory>,
    private val streamProvisioner: StreamShardProvisioner = NoopStreamShardProvisioner,
    private val clock: Clock = Clock.systemUTC(),
    private val metrics: CoordinatorMetrics = NoopCoordinatorMetrics,
    private val stateMutex: CoordinatorStateMutex = LocalCoordinatorStateMutex,
    private val redisCommands: CoordinatorRedisCommands = CoordinatorRedisCommands(
        redisConnectionFactory = redisConnectionFactory.ifAvailable,
    ),
    private val objectMapper: ObjectMapper = ObjectMapper(),
) {
    private val shardRateSnapshots = ConcurrentHashMap<ShardRateKey, ShardRateSnapshot>()
    private val monitoringOffsetCache = ConcurrentHashMap<MonitoringOffsetCacheKey, MonitoringOffsetCacheEntry>()
    private val monitoringOffsetLocks = ConcurrentHashMap<MonitoringOffsetCacheKey, Any>()
    private val monitoringOffsetRefreshInFlight = ConcurrentHashMap<MonitoringOffsetCacheKey, AtomicBoolean>()
    private val redisHealthRefreshInFlight = AtomicBoolean(false)
    @Volatile
    private var redisHealthCache: RedisHealthCacheEntry? = null
    private val monitoringGroupPermits = Semaphore(properties.monitoring.groupQueryParallelism.coerceAtLeast(1))
    private val monitoringShardPermits = Semaphore(properties.monitoring.shardQueryParallelism.coerceAtLeast(1))
    private val monitoringGroupExecutor = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name("redis-stream-coordinator-monitoring-group-", 0).factory(),
    )
    private val monitoringExecutor = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name("redis-stream-coordinator-monitoring-", 0).factory(),
    )

    /**
     * Creates a coordinator group and provisions the initial stream shards.
     */
    @CriticalSection(operation = "create-group")
    fun createGroup(streamPrefix: String, consumerGroup: String, request: CreateGroupRequest): GroupResponse =
        createGroupOnce(streamPrefix, consumerGroup, request)

    /**
     * Creates a stream-level shard layout through the public operator API.
     */
    @CriticalSection(operation = "create-stream")
    fun createStream(streamPrefix: String, request: CreateStreamRequest): StreamCreateResponse {
        val existingGroups = stateStore.list().filter { it.streamPrefix == streamPrefix }
        if (existingGroups.isNotEmpty()) {
            throw CoordinatorException(
                CoordinatorError.STREAM_PREFIX_ALREADY_EXISTS,
                "Stream prefix '$streamPrefix' is already managed by coordinator metadata",
            )
        }
        val response = createGroupOnce(streamPrefix, defaultStreamConsumerGroup(streamPrefix), request.toGroupRequest())
        return StreamCreateResponse(
            streamPrefix = response.streamPrefix,
            shardCount = response.shardCount,
            metadataVersion = response.metadataVersion,
        )
    }

    /**
     * Performs the create operation after the cross-instance state mutex has been acquired.
     */
    private fun createGroupOnce(streamPrefix: String, consumerGroup: String, request: CreateGroupRequest): GroupResponse {
        val key = GroupKey(streamPrefix, consumerGroup)
        if (stateStore.contains(key)) {
            throw CoordinatorException(CoordinatorError.GROUP_ALREADY_EXISTS)
        }

        val now = Instant.now(clock)
        val shardCount = request.initialShardCount ?: properties.defaults.initialShardCount
        if (stateStore.list().none { it.streamPrefix == streamPrefix }) {
            requireStreamPrefixNotAlreadyMaterialized(streamPrefix, shardCount)
        }
        val group = GroupMetadata(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            groupEpoch = 1,
            metadataVersion = 1,
            assignmentEpoch = 0,
            state = GroupState.EMPTY,
            shardCount = shardCount,
            createdAt = now,
            updatedAt = now,
        )
        reconcile(group, now)
        if (!stateStore.putIfAbsent(key, group)) {
            throw CoordinatorException(CoordinatorError.GROUP_ALREADY_EXISTS)
        }
        // Provision after the state claim is won so rejected concurrent creates cannot leave stream keys behind.
        try {
            streamProvisioner.provision(group.streamShardProvisioningPlan())
        } catch (error: RuntimeException) {
            runCatching { stateStore.deleteIfRevision(key, group.storeRevision) }
                .exceptionOrNull()
                ?.let(error::addSuppressed)
            throw error
        }
        recordGroupState(group)
        return group.toResponse()
    }

    private fun requireStreamPrefixNotAlreadyMaterialized(streamPrefix: String, shardCount: Int) {
        if (!properties.streams.provisioningEnabled || !redisCommands.isConfigured()) {
            return
        }

        val existingKeys = (listOf(streamPrefix) + RedisStreamShardKeys.forShardCount(streamPrefix, shardCount).map { it.value })
            .filter(redisCommands::hasKey)
        if (existingKeys.isNotEmpty()) {
            throw CoordinatorException(
                CoordinatorError.STREAM_PREFIX_ALREADY_EXISTS,
                "Stream prefix '$streamPrefix' already has Redis key(s): ${existingKeys.joinToString(", ")}",
            )
        }
    }

    private fun defaultStreamConsumerGroup(streamPrefix: String): String =
        streamPrefix

    /**
     * Returns the current group metadata snapshot without mutating coordinator state.
     */
    fun getGroup(streamPrefix: String, consumerGroup: String): GroupResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        recordGroupState(group)
        return group.toResponse()
    }

    /**
     * Deletes group metadata after checking live members and the current store revision.
     */
    @CriticalSection(operation = "delete-group")
    fun deleteGroup(streamPrefix: String, consumerGroup: String, request: DeleteGroupRequest): GroupResponse =
        withStateConflictRetry("delete-group") {
            val group = requireGroup(streamPrefix, consumerGroup)
            val hasLiveMembers = group.members.values.any { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }
            if (hasLiveMembers && !request.force) {
                throw CoordinatorException(CoordinatorError.GROUP_HAS_ACTIVE_MEMBERS)
            }
            val response = group.toResponse()
            if (!stateStore.deleteIfRevision(group.key(), group.storeRevision)) {
                throw CoordinatorStateConflictException("Coordinator state changed before delete for ${group.key()}")
            }
            response
        }

    /**
     * Returns producer routing metadata for the current shard layout.
     */
    fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse {
        try {
            val group = requireGroup(streamPrefix, consumerGroup)
            streamProvisioner.provision(group.streamShardProvisioningPlan())
            recordGroupState(group)
            val response = group.toProducerRoutingResponse()
            metrics.recordProducerRouting(streamPrefix, consumerGroup, "SUCCESS")
            return response
        } catch (error: RuntimeException) {
            metrics.recordProducerRouting(streamPrefix, consumerGroup, "ERROR")
            throw error
        }
    }

    /**
     * Starts or resumes a shard-count change.
     */
    @CriticalSection(operation = "scale-group")
    fun scaleGroup(streamPrefix: String, consumerGroup: String, request: ScaleGroupRequest): Migration {
        try {
            val migration = withStateConflictRetry("scale-group") { scaleGroupOnce(streamPrefix, consumerGroup, request) }
            metrics.recordScaleRequest(
                streamPrefix,
                consumerGroup,
                if (migration.reshardingId == "noop") "NOOP" else "SUCCESS",
            )
            return migration
        } catch (error: RuntimeException) {
            metrics.recordScaleRequest(streamPrefix, consumerGroup, "ERROR")
            throw error
        }
    }

    /**
     * Changes the stream shard count for every consumer group registered under a stream prefix.
     */
    @CriticalSection(operation = "scale-stream")
    fun scaleStream(streamPrefix: String, request: ScaleStreamRequest): StreamScaleResponse {
        try {
            val response = withStateConflictRetry("scale-stream") { scaleStreamOnce(streamPrefix, request) }
            response.migrations.forEach { migration ->
                metrics.recordScaleRequest(
                    streamPrefix,
                    "stream",
                    if (migration.reshardingId == "noop") "NOOP" else "SUCCESS",
                )
            }
            return response
        } catch (error: RuntimeException) {
            metrics.recordScaleRequest(streamPrefix, "stream", "ERROR")
            throw error
        }
    }

    private fun scaleStreamOnce(streamPrefix: String, request: ScaleStreamRequest): StreamScaleResponse {
        val groups = stateStore.list()
            .filter { it.streamPrefix == streamPrefix }
            .sortedBy { it.consumerGroup }
        if (groups.isEmpty()) {
            throw CoordinatorException(
                CoordinatorError.STREAM_NOT_FOUND,
                "No coordinator groups exist for stream prefix $streamPrefix",
            )
        }

        groups.forEach { group ->
            requireNoMetadataCorrection(group)
            val activeMigration = group.activeReshardingId?.let { group.migrations[it] }
            if (activeMigration != null &&
                !(activeMigration.state == MigrationState.PREPARING &&
                    activeMigration.toShardCount == request.targetShardCount)
            ) {
                throw CoordinatorException(
                    CoordinatorError.ACTIVE_MIGRATION_EXISTS,
                    "Consumer group ${group.consumerGroup} already has an active resharding",
                )
            }
        }

        val groupRequest = request.toGroupRequest()
        val migrations = groups.map { group ->
            scaleGroupOnce(streamPrefix, group.consumerGroup, groupRequest)
        }
        return StreamScaleResponse(
            streamPrefix = streamPrefix,
            targetShardCount = request.targetShardCount,
            affectedConsumerGroups = groups.map { it.consumerGroup },
            migrations = migrations,
        )
    }

    /**
     * Builds the PREPARING resharding record before provisioning or activating new stream shards.
     */
    private fun scaleGroupOnce(streamPrefix: String, consumerGroup: String, request: ScaleGroupRequest): Migration {
        val group = requireGroup(streamPrefix, consumerGroup)
        requireNoMetadataCorrection(group)
        val now = Instant.now(clock)
        expireMembers(group, now)

        val activeMigration = group.activeReshardingId?.let { group.migrations[it] }
        if (activeMigration != null) {
            if (activeMigration.state == MigrationState.PREPARING &&
                activeMigration.toShardCount == request.targetShardCount
            ) {
                return provisionAndActivatePreparedMigration(group, activeMigration, request, now)
            }
            throw CoordinatorException(CoordinatorError.ACTIVE_MIGRATION_EXISTS)
        }

        val fromShardCount = group.shardCount
        if (fromShardCount == request.targetShardCount) {
            stateStore.save(group.key(), group)
            return Migration(
                reshardingId = "noop",
                fromShardCount = fromShardCount,
                toShardCount = fromShardCount,
                state = MigrationState.DEPRECATED,
                createdAt = now,
                updatedAt = now,
            )
        }

        val migration = Migration(
            reshardingId = newReshardingId(),
            fromShardCount = fromShardCount,
            toShardCount = request.targetShardCount,
            state = MigrationState.PREPARING,
            createdAt = now,
            updatedAt = now,
        )

        group.migrations[migration.reshardingId] = migration
        group.activeReshardingId = migration.reshardingId
        group.updatedAt = now
        stateStore.save(group.key(), group)
        return provisionAndActivatePreparedMigration(group, migration, request, now)
    }

    /**
     * Provisions the target shard layout, switches producer routing, and reconciles consumers.
     */
    private fun provisionAndActivatePreparedMigration(
        group: GroupMetadata,
        migration: Migration,
        request: ScaleGroupRequest,
        now: Instant,
    ): Migration {
        // Commit PREPARING first; conflict retries must not create Redis shards without matching coordinator state.
        streamProvisioner.provision(
            RedisStreamShardProvisioningPlan.forShardCount(
                streamPrefix = group.streamPrefix,
                consumerGroup = group.consumerGroup,
                shardCount = migration.toShardCount,
            ),
        )

        migration.state = MigrationState.ACTIVE
        migration.updatedAt = now
        group.shardCount = migration.toShardCount
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        enforceRebalanceTimeouts(group, now)
        stateStore.save(group.key(), group)
        recordGroupState(group)
        return migration
    }

    /**
     * Returns one resharding record by id.
     */
    fun getMigration(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration =
        requireGroup(streamPrefix, consumerGroup).migrations[reshardingId]
            ?: throw CoordinatorException(CoordinatorError.MIGRATION_NOT_FOUND)

    /**
     * Rolls back an active resharding operation before the previous shard count has been deprecated.
     */
    @CriticalSection(operation = "rollback-migration")
    fun rollbackMigration(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration =
        withStateConflictRetry("rollback-migration") { rollbackMigrationOnce(streamPrefix, consumerGroup, reshardingId) }

    /**
     * Restores active writes and reads to the previous shard count.
     */
    private fun rollbackMigrationOnce(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration {
        val group = requireGroup(streamPrefix, consumerGroup)
        requireNoMetadataCorrection(group)
        val migration = group.migrations[reshardingId]
            ?: throw CoordinatorException(CoordinatorError.MIGRATION_NOT_FOUND)

        if (migration.state == MigrationState.ROLLED_BACK || migration.state == MigrationState.ROLLING_BACK) {
            return migration
        }
        if (group.activeReshardingId != reshardingId || migration.state != MigrationState.ACTIVE) {
            throw CoordinatorException(CoordinatorError.ROLLBACK_NOT_ALLOWED)
        }

        val now = Instant.now(clock)
        migration.state = MigrationState.ROLLED_BACK
        migration.updatedAt = now
        group.activeReshardingId = null
        group.shardCount = migration.fromShardCount
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        stateStore.save(group.key(), group)
        recordGroupState(group)
        return migration
    }

    /**
     * Processes a consumer heartbeat and returns assignment, fencing, or retry instructions.
     */
    @CriticalSection(operation = "heartbeat")
    fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse {
        val response = withStateConflictRetry("heartbeat") { handleHeartbeat(streamPrefix, consumerGroup, memberId, request) }
        metrics.recordHeartbeat(streamPrefix, consumerGroup, response.status)
        return response
    }

    /**
     * Applies join, leave, lease renewal, ownership validation, and assignment reconciliation.
     */
    private fun handleHeartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse {
        if (!CoordinatorProtocol.support(request.protocolVersion)) {
            return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNSUPPORTED_PROTOCOL)
        }
        if (request.memberId != memberId) {
            return rejectedHeartbeat(request, memberId, HeartbeatStatus.INVALID_REQUEST)
        }

        val group = try {
            requireGroup(streamPrefix, consumerGroup)
        } catch (error: CoordinatorException) {
            if (error.error == CoordinatorError.GROUP_NOT_FOUND) {
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNKNOWN_MEMBER_ID)
            }
            throw error
        }

        val now = Instant.now(clock)
        val membersExpired = expireMembers(group, now)
        val membersPruned = pruneStaleMembers(group, now)
        val maintenanceChanged = membersExpired > 0 || membersPruned > 0
        val existing = group.members[memberId]
        val metadataSyncMember = existing
            ?.takeUnless { it.state == MemberState.FENCED || it.state == MemberState.EXPIRED }
        if (request.metadataVersion > group.metadataVersion && metadataSyncMember != null) {
            startMetadataCorrection(group, request.metadataVersion, now)
            refreshMemberLivenessForMetadataSync(group, metadataSyncMember, request, now)
            if (maintenanceChanged) {
                reconcile(group, now)
            }
            val response = metadataSyncHeartbeat(group, request, memberId, metadataSyncMember)
            stateStore.save(group.key(), group)
            recordGroupState(group)
            return response
        }
        if (group.metadataCorrection != null && metadataSyncMember != null && request.metadataVersion != group.metadataVersion) {
            refreshMemberLivenessForMetadataSync(group, metadataSyncMember, request, now)
            if (maintenanceChanged) {
                reconcile(group, now)
            }
            val response = metadataSyncHeartbeat(group, request, memberId, metadataSyncMember)
            stateStore.save(group.key(), group)
            recordGroupState(group)
            return response
        }
        val member = when {
            request.memberEpoch < -1L -> {
                if (maintenanceChanged) {
                    stateStore.save(group.key(), group)
                }
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.INVALID_REQUEST)
            }
            existing == null && request.memberEpoch == 0L -> registerOrRejoinMember(group, memberId, request, now)
            existing == null && request.memberEpoch == -1L ->
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNKNOWN_MEMBER_ID)
            existing == null -> return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNKNOWN_MEMBER_ID)
            request.memberEpoch == 0L &&
                (existing.state == MemberState.EXPIRED || existing.state == MemberState.FENCED || existing.state == MemberState.LEAVING) ->
                registerOrRejoinMember(group, memberId, request, now)
            request.memberEpoch == 0L && isInitialJoinReplay(existing, request) -> {
                refreshMemberLiveness(group, existing, request, now)
                if (maintenanceChanged) {
                    reconcile(group, now)
                }
                val response = initialJoinReplayHeartbeat(group, request, existing)
                stateStore.save(group.key(), group)
                recordGroupState(group)
                return response
            }
            request.memberEpoch == 0L -> {
                if (maintenanceChanged) {
                    stateStore.save(group.key(), group)
                }
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.INVALID_REQUEST)
            }
            existing.state == MemberState.FENCED || existing.state == MemberState.EXPIRED -> {
                if (maintenanceChanged) {
                    stateStore.save(group.key(), group)
                }
                return fencedHeartbeat(group, request, memberId, existing)
            }
            request.memberEpoch == -1L -> markLeaving(group, memberId, request, now)
            request.memberEpoch > existing.memberEpoch -> {
                if (maintenanceChanged) {
                    stateStore.save(group.key(), group)
                }
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.INVALID_REQUEST)
            }
            request.memberEpoch < existing.memberEpoch -> {
                if (maintenanceChanged) {
                    stateStore.save(group.key(), group)
                }
                return fencedHeartbeat(group, request, memberId, existing)
            }
            else -> existing
        }

        val correctionWasActive = group.metadataCorrection != null
        val validatingRequest = metadataCorrectionAwareRequest(group, member, request)
        val ownershipReport = validateOwnershipReport(group, member, validatingRequest)
            ?: return fenceInvalidOwnershipReport(group, member, request, now)

        member.memberName = request.memberName ?: member.memberName
        member.metadataVersion = group.metadataVersion
        member.runtimeMaxConcurrency = request.runtimeConsumerCapacity.runtimeMaxConcurrency
        member.activeConsumerWorkers =
            (request.runtimeConsumerCapacity.runtimeMaxConcurrency - request.runtimeConsumerCapacity.availableConcurrency)
                .coerceAtLeast(0)
        member.rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis()
        member.currentAssignment = if (member.state == MemberState.LEAVING) emptySet() else ownershipReport.ownedShards
        if (member.state == MemberState.LEAVING) {
            member.grantedAssignment = emptySet()
        }
        member.revoking = ownershipReport.revokingShards
            .filterNot { it.state == RevokingShardState.REVOKED && it.inFlight == 0 }
            .map { it.shard }
            .toSet()
        member.shardProgress = ownershipReport.shardProgress
        member.lastHeartbeatAt = now
        member.memberLeaseExpiresAt = now.plus(properties.memberLeaseTtl)

        reconcile(group, now)
        enforceRebalanceTimeouts(group, now)
        advanceMigrationDrainState(group, now)
        if (correctionWasActive) {
            acknowledgeMetadataCorrectionIfNeeded(group, member.memberId, request, now)
        }
        if (member.state == MemberState.FENCED) {
            stateStore.save(group.key(), group)
            return fencedHeartbeat(group, request, memberId, member)
        }
        if (member.state == MemberState.ACTIVE || member.state == MemberState.STARTING) {
            member.memberEpoch = group.assignmentEpoch
        }
        val target = group.targetAssignments[memberId].orEmpty()
        val blocked = blockedShards(group, memberId)
        val assigned = target.filterNot { it in blocked }.toSortedSet()
        val pending = target.filter { it in blocked }.toSortedSet()
        if (group.metadataCorrection != null) {
            val response = revokePendingHeartbeat(group, request, member, target, blocked)
            stateStore.save(group.key(), group)
            recordGroupState(group)
            return response
        }
        member.grantedAssignment = assigned

        val response = HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.OK,
            memberId = memberId,
            memberEpoch = member.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignment = AssignmentView(
                assignedShards = assigned,
                pendingShards = pending,
                metadataVersion = group.metadataVersion,
            ),
        )
        stateStore.save(group.key(), group)
        recordGroupState(group)
        return response
    }

    /**
     * Reports coordinator liveness and Redis dependency status.
     */
    fun health(): HealthResponse {
        val redisStatus = if (requiresRedis()) {
            redisHealthStatus()
        } else {
            "NOT_CONFIGURED"
        }

        val health = HealthResponse(
            status = if (redisStatus == "DOWN") "DEGRADED" else "UP",
            coordinatorId = properties.id,
            redis = redisStatus,
            loop = "UP",
        )
        metrics.recordHealth(health.status == "UP")
        return health
    }

    private fun redisHealthStatus(): String {
        val now = Instant.now(clock)
        redisHealthCache
            ?.takeIf { !now.isAfter(it.expiresAt) }
            ?.let { return it.status }

        refreshRedisHealthAsync()
        return redisHealthCache?.status ?: "UNKNOWN"
    }

    private fun refreshRedisHealthAsync() {
        if (!redisHealthRefreshInFlight.compareAndSet(false, true)) {
            return
        }
        CompletableFuture.runAsync(
            {
                try {
                    val status = pingRedisForHealth()
                    val ttlMs = properties.health.cacheTtlMs.coerceAtLeast(0)
                    redisHealthCache = RedisHealthCacheEntry(
                        expiresAt = Instant.now(clock).plusMillis(ttlMs),
                        status = status,
                    )
                } finally {
                    redisHealthRefreshInFlight.set(false)
                }
            },
            healthExecutor,
        )
    }

    private fun redisPingTimeoutMs(): Long =
        properties.health.redisTimeoutMs.coerceAtLeast(1)

    private fun redisHealthCacheTtlMs(): Long {
        val configured = properties.health.cacheTtlMs
        return if (configured > 0) configured else redisPingTimeoutMs()
    }

    private fun cacheRedisHealth(status: String) {
        redisHealthCache = RedisHealthCacheEntry(
            expiresAt = Instant.now(clock).plusMillis(redisHealthCacheTtlMs()),
            status = status,
        )
    }

    private fun completeRedisHealth(status: String): String {
        cacheRedisHealth(status)
        return status
    }

    private fun failRedisHealth(): String {
        cacheRedisHealth("DOWN")
        return "DOWN"
    }

    private fun pingRedisForHealth(): String {
        redisConnectionFactory.ifAvailable ?: return "NOT_CONFIGURED"
        val timeoutMs = redisPingTimeoutMs()
        val ping = CompletableFuture.supplyAsync(
            { redisCommands.ping() },
            healthExecutor,
        )
        return runCatching {
            ping.get(timeoutMs, TimeUnit.MILLISECONDS)
        }.fold(
            onSuccess = { completeRedisHealth("UP") },
            onFailure = {
                ping.cancel(true)
                failRedisHealth()
            },
        )
    }

    /**
     * Returns module-defined protocol compatibility metadata for operators and client diagnostics.
     */
    fun compatibility(): CoordinatorCompatibilityResponse =
        CoordinatorProtocol.compatibility()

    /**
     * Detects a retry of the first join heartbeat after the original response was lost.
     */
    private fun isInitialJoinReplay(existing: MemberMetadata, request: HeartbeatRequest): Boolean =
        (existing.state == MemberState.ACTIVE || existing.state == MemberState.STARTING) &&
            existing.currentAssignment.isEmpty() &&
            existing.revoking.isEmpty() &&
            request.ownedShards.isEmpty() &&
            request.revokingShards.isEmpty() &&
            request.shardProgress.isEmpty()

    private fun requiresRedis(): Boolean =
        properties.store.type == CoordinatorProperties.StoreType.REDIS ||
            properties.streams.provisioningEnabled ||
            properties.audit.sink == CoordinatorProperties.AuditSink.REDIS

    companion object {
        private const val MAX_MESSAGE_PAGE_SIZE = 1_000
        private const val MESSAGE_LAST_PAGE_CURSOR = "__rsc_last__"
        private const val MESSAGE_TAIL_PAGE_CURSOR_PREFIX = "__rsc_tail__:"

        private val healthExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("redis-stream-coordinator-health-", 0).factory(),
        )
    }

    /**
     * Runs one background maintenance pass without blocking if another coordinator owns the mutex.
     */
    @Synchronized
    fun tick(): CoordinatorTickResult {
        return stateMutex.tryCriticalSection("tick") {
            tickOnce()
        } ?: CoordinatorTickResult(scannedGroups = 0, changedGroups = 0)
    }

    /**
     * Expires leases, advances migrations, and records metrics for all known groups.
     */
    private fun tickOnce(): CoordinatorTickResult {
        val startedAt = Instant.now(clock)
        val now = Instant.now(clock)
        val groups = stateStore.list()
        var changedGroups = 0
        groups.forEach { group ->
            if (withStateConflictRetry("tick-refresh") { refreshGroupOperationalState(group.key(), now) }) {
                changedGroups += 1
            }
        }
        val result = CoordinatorTickResult(scannedGroups = groups.size, changedGroups = changedGroups)
        metrics.recordTick(result, Duration.between(startedAt, Instant.now(clock)))
        return result
    }

    /**
     * Lists group metadata snapshots without mutating coordinator state.
     */
    fun listGroups(): GroupsResponse =
        GroupsResponse(stateStore.list().map {
            recordGroupState(it)
            it.toResponse()
        })

    /**
     * Lists members in a group from the latest stored metadata snapshot.
     */
    fun listMembers(streamPrefix: String, consumerGroup: String): MembersResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        recordGroupState(group)
        return MembersResponse(group.members.values.sortedBy { it.memberId })
    }

    /**
     * Returns target/current assignment snapshots plus invariant violations for debugging.
     */
    fun assignments(streamPrefix: String, consumerGroup: String): AssignmentsResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        val violations = invariantViolations(group)
        metrics.recordGroupState(group, violations.size)
        return AssignmentsResponse(
            targetAssignment = group.targetAssignments.mapValues { it.value.toSortedSet() },
            currentAssignments = group.members.mapValues { it.value.currentAssignment.toSortedSet() },
            revokeProgress = group.members.mapValues { it.value.revoking.toSortedSet() }
                .filterValues { it.isNotEmpty() },
            invariantViolations = violations,
        )
    }

    /**
     * Returns the last consumption progress reported by consumers for each owned stream shard.
     */
    fun consumptionProgress(streamPrefix: String, consumerGroup: String): ConsumptionProgressResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        recordGroupState(group)
        return ConsumptionProgressResponse(group.toConsumerShardProgress())
    }

    /**
     * Returns Kafka-style partition monitoring data: end offsets, consumer offsets, pending counts, and lag per shard.
     */
    fun streamShardOffsets(streamPrefix: String, consumerGroup: String): StreamShardOffsetsResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        recordGroupState(group)
        return group.toStreamShardOffsets().also(metrics::recordStreamShardOffsets)
    }

    /**
     * Reads a page of Redis Stream records for the selected shard without changing consumer group state.
     */
    fun streamMessages(
        streamPrefix: String,
        consumerGroup: String,
        shardIndex: Int,
        direction: StreamMessagePageDirection,
        cursor: String?,
        limit: Int,
    ): StreamMessagesPageResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        return readStreamMessages(group, shardIndex, direction, cursor, limit)
    }

    /**
     * Returns a flat group summary table for Grafana REST data-source panels.
     */
    fun grafanaGroups(): List<GrafanaGroupRow> =
        stateStore.list().map { group ->
            metrics.recordGroupState(group, invariantViolations(group).size)
            val offsets = group.toCachedStreamShardOffsets().also(metrics::recordStreamShardOffsets)
            val rates = observeShardRates(offsets).values
            val currentShards = group.members.values.filter { it.isLiveOwner() }.sumOf { it.currentAssignment.size }
            GrafanaGroupRow(
                streamPrefix = group.streamPrefix,
                consumerGroup = group.consumerGroup,
                state = group.state,
                groupEpoch = group.groupEpoch,
                assignmentEpoch = group.assignmentEpoch,
                metadataVersion = group.metadataVersion,
                shardCount = group.shardCount,
                activeMembers = group.members.values.count { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING },
                totalMembers = group.members.size,
                currentShards = currentShards,
                targetShards = group.targetAssignments.values.sumOf { it.size },
                assignedShardRatio = "$currentShards / ${group.shardCount}",
                revokingShards = group.members.values.filter { it.isLiveOwner() }.sumOf { it.revoking.size },
                totalStreamLength = offsets.shards.sumOf { it.streamLength },
                totalPendingCount = offsets.shards.sumOf { it.pendingCount.coerceAtLeast(0) },
                totalLag = offsets.totalLag?.coerceAtLeast(0),
                producedPerSecond = rates.sumKnownOrNull { it.producedPerSecond },
                consumedPerSecond = rates.sumKnownOrNull { it.consumedPerSecond },
                totalMemoryUsageBytes = offsets.totalMemoryUsageBytes,
                memoryUsageKnown = offsets.memoryUsageKnown,
            )
        }

    /**
     * Returns sharded stream prefixes that currently have coordinator metadata.
     */
    fun grafanaStreamOptions(): List<GrafanaOptionRow> =
        stateStore.list()
            .map { it.streamPrefix }
            .distinct()
            .sorted()
            .map { streamPrefix ->
                GrafanaOptionRow(text = streamPrefix, value = streamPrefix)
            }

    /**
     * Returns consumer-group options, optionally scoped by stream prefix for chained Grafana variables.
     */
    fun grafanaConsumerGroupOptions(streamPrefix: String?): List<GrafanaOptionRow> =
        stateStore.list()
            .asSequence()
            .filter { group -> streamPrefix.isNullOrBlank() || group.streamPrefix == streamPrefix }
            .sortedWith(compareBy<GroupMetadata> { it.streamPrefix }.thenBy { it.consumerGroup })
            .map { group ->
                GrafanaOptionRow(
                    text = "${group.streamPrefix} / ${group.consumerGroup}",
                    value = group.consumerGroup,
                )
            }
            .toList()

    /**
     * Returns shard indexes for the selected stream/group without querying Redis Stream offsets.
     */
    fun grafanaShardOptions(streamPrefix: String, consumerGroup: String): List<GrafanaOptionRow> {
        if (streamPrefix.isBlank() || consumerGroup.isBlank()) {
            return emptyList()
        }
        val group = requireGroup(streamPrefix, consumerGroup)
        return (0 until group.shardCount).map { shardIndex ->
            GrafanaOptionRow(text = ":$shardIndex", value = shardIndex.toString())
        }
    }

    /**
     * Returns a flat member table for Grafana REST data-source panels.
     */
    fun grafanaMembers(streamPrefix: String, consumerGroup: String): List<GrafanaMemberRow> =
        if (streamPrefix.isBlank() || consumerGroup.isBlank()) {
            emptyList()
        } else {
            requireGroup(streamPrefix, consumerGroup).let { group ->
                metrics.recordGroupState(group, invariantViolations(group).size)
                group.members.values.sortedBy { it.memberId }.map { member ->
                    GrafanaMemberRow(
                        streamPrefix = group.streamPrefix,
                        consumerGroup = group.consumerGroup,
                        memberId = member.memberId,
                        memberName = member.memberName,
                        state = member.state,
                        memberEpoch = member.memberEpoch,
                        metadataVersion = member.metadataVersion,
                        currentShardCount = member.currentAssignment.size,
                        revokingShardCount = member.revoking.size,
                        runtimeMaxConcurrency = member.runtimeMaxConcurrency,
                        activeConsumerWorkers = member.activeConsumerWorkers,
                        lastHeartbeatAt = member.lastHeartbeatAt,
                        memberLeaseExpiresAt = member.memberLeaseExpiresAt,
                    )
                }
            }
        }

    /**
     * Returns flat shard offset rows for Grafana REST data-source panels.
     */
    fun grafanaShards(streamPrefix: String, consumerGroup: String): List<GrafanaShardRow> {
        val groups = monitoringGroups(streamPrefix, consumerGroup)
        if (groups.size <= 1) {
            return groups.flatMap { it.toGrafanaShardRows() }
        }
        return groups.map { group ->
            CompletableFuture.supplyAsync({
                monitoringGroupPermits.withPermit {
                    group.toGrafanaShardRows()
                }
            }, monitoringGroupExecutor)
        }.flatMap { future ->
            future.get()
        }
    }

    /**
     * Returns flat target/current/revoking assignment rows for Grafana REST data-source panels.
     */
    fun grafanaAssignments(streamPrefix: String, consumerGroup: String): List<GrafanaAssignmentRow> =
        if (streamPrefix.isBlank() || consumerGroup.isBlank()) {
            emptyList()
        } else {
            requireGroup(streamPrefix, consumerGroup).let { group ->
                metrics.recordGroupState(group, invariantViolations(group).size)
                val currentOwners = group.members.values
                    .filter { it.isLiveOwner() }
                    .flatMap { member -> member.currentAssignment.map { shard -> shard to member.memberId } }
                    .groupBy({ it.first }, { it.second })
                val revokingOwners = group.members.values
                    .filter { it.isLiveOwner() }
                    .flatMap { member -> member.revoking.map { shard -> shard to member.memberId } }
                    .groupBy({ it.first }, { it.second })
                group.readableShards().map { shard ->
                    GrafanaAssignmentRow(
                        streamPrefix = group.streamPrefix,
                        consumerGroup = group.consumerGroup,
                        shardIndex = shard.shardIndex,
                        targetOwners = group.targetAssignments
                            .filterValues { shard in it }
                            .keys
                            .sorted()
                            .joinToString(","),
                        currentOwners = currentOwners[shard].orEmpty().sorted().joinToString(","),
                        revokingOwners = revokingOwners[shard].orEmpty().sorted().joinToString(","),
                    )
                }
            }
        }

    private fun monitoringGroups(streamPrefix: String, consumerGroup: String): List<GroupMetadata> =
        if (streamPrefix.isBlank() && consumerGroup.isBlank()) {
            stateStore.list()
        } else if (streamPrefix.isBlank() || consumerGroup.isBlank()) {
            stateStore.list().filter { group ->
                (streamPrefix.isBlank() || group.streamPrefix == streamPrefix) &&
                    (consumerGroup.isBlank() || group.consumerGroup == consumerGroup)
            }
        } else {
            listOf(requireGroup(streamPrefix, consumerGroup))
        }

    private fun GroupMetadata.toGrafanaShardRows(): List<GrafanaShardRow> {
        metrics.recordGroupState(this, invariantViolations(this).size)
        val offsets = toCachedStreamShardOffsets().also(metrics::recordStreamShardOffsets)
        val rates = observeShardRates(offsets)
        val targetOwners = targetOwnersByShard()
        return offsets.shards.map { offset ->
            val currentOwnerIds = offset.ownerMemberIds.sorted()
            val targetOwnerIds = targetOwners[offset.shard].orEmpty().sorted()
            val rate = rates[offset.shard]
            GrafanaShardRow(
                streamPrefix = offset.streamPrefix,
                consumerGroup = offset.consumerGroup,
                shardCount = shardCount,
                shardIndex = offset.shard.shardIndex,
                shardLabel = ":${offset.shard.shardIndex}",
                streamKey = offset.streamKey,
                redisSlot = offset.redisSlot,
                redisNodeEndpoint = offset.redisNodeEndpoint,
                redisNodeId = offset.redisNodeId,
                redisSlotRangeStart = offset.redisSlotRangeStart,
                redisSlotRangeEnd = offset.redisSlotRangeEnd,
                streamLength = offset.streamLength,
                firstRecordId = offset.firstRecordId,
                lastRecordId = offset.lastRecordId,
                lastGeneratedId = offset.lastGeneratedId,
                groupLastDeliveredId = offset.groupLastDeliveredId,
                consumerLastDeliveredId = offset.consumerLastDeliveredId,
                consumerLastAckedId = offset.consumerLastAckedId,
                pendingCount = offset.pendingCount,
                lag = offset.lag?.coerceAtLeast(0),
                lagKnown = offset.lag != null,
                producedPerSecond = rate?.producedPerSecond,
                consumedPerSecond = rate?.consumedPerSecond,
                memoryUsageBytes = offset.memoryUsageBytes,
                memoryUsageKnown = offset.memoryUsageBytes != null,
                targetOwnerMemberIds = targetOwnerIds.joinToString(","),
                currentOwnerMemberIds = currentOwnerIds.joinToString(","),
                ownerState = ownerState(currentOwnerIds, targetOwnerIds),
                ownerMemberIds = currentOwnerIds.joinToString(","),
            )
        }
    }

    private fun observeShardRates(offsets: StreamShardOffsetsResponse): Map<ShardId, ShardRate> {
        val observedAt = Instant.now(clock)
        return offsets.shards.associate { offset ->
            val key = ShardRateKey(offset.streamPrefix, offset.consumerGroup, offset.shard)
            val current = ShardRateSnapshot(
                observedAt = observedAt,
                streamLength = offset.streamLength.coerceAtLeast(0),
                lag = offset.lag?.coerceAtLeast(0),
            )
            val previous = shardRateSnapshots.put(key, current)
            offset.shard to current.rateFrom(previous)
        }
    }

    private fun GroupMetadata.targetOwnersByShard(): Map<ShardId, List<String>> =
        targetAssignments
            .flatMap { (memberId, shards) -> shards.map { shard -> shard to memberId } }
            .groupBy({ it.first }, { it.second })

    private fun ownerState(currentOwners: List<String>, targetOwners: List<String>): String =
        when {
            currentOwners.isEmpty() && targetOwners.isEmpty() -> "UNASSIGNED"
            currentOwners.isEmpty() -> "PENDING_ACK"
            currentOwners.toSet() == targetOwners.toSet() -> "CURRENT"
            else -> "TRANSITIONING"
        }

    /**
     * Returns flat stream message rows for Grafana REST data-source panels.
     */
    fun grafanaMessages(
        streamPrefix: String,
        consumerGroup: String,
        shardIndex: String,
        direction: StreamMessagePageDirection,
        cursor: String?,
        recordId: String?,
        limit: Int,
    ): List<GrafanaMessageRow> {
        if (streamPrefix.isBlank() || consumerGroup.isBlank()) {
            return emptyList()
        }
        val group = requireGroup(streamPrefix, consumerGroup)
        val exactRecordId = recordId?.trim()?.takeIf { it.isNotEmpty() }
        val tailPageCursor = parseTailMessageCursor(cursor)
        val shardSelector = if (exactRecordId != null) {
            "all"
        } else {
            shardIndex.takeIf { it.isNotBlank() } ?: "0"
        }
        val selectedShards = selectedMessageShards(group, shardSelector)
        val pageLimit = normalizeMessagePageLimit(limit)
        val totalMessages = selectedShards.sumOf { shard ->
            redisCommands.xInfoStream(RedisStreamShardKeys.forShard(group.streamPrefix, shard.shardIndex).value).length
        }
        if (exactRecordId != null) {
            return searchStreamMessagesByRecordId(
                group = group,
                shards = selectedShards,
                recordId = exactRecordId,
                direction = direction,
                pageLimit = pageLimit,
                shardSelector = shardSelector,
                totalMessages = totalMessages,
            )
        }
        return if (selectedShards.size == 1) {
            val shard = selectedShards.single()
            val page = if (tailPageCursor != null) {
                readTailStreamMessages(
                    group = group,
                    shardIndex = shard.shardIndex,
                    direction = direction,
                    limit = pageLimit,
                    totalMessages = totalMessages,
                    distanceFromLast = tailPageCursor,
                )
            } else {
                readStreamMessages(group, shard.shardIndex, direction, cursor, pageLimit)
            }
            page.records.map { record ->
                record.toGrafanaMessageRow(
                    page = page,
                    shardSelector = shardSelector,
                    nextCursor = page.nextCursor,
                    totalMessages = totalMessages,
                )
            }
        } else {
            if (tailPageCursor != null) {
                readTailMergedStreamMessages(
                    group = group,
                    shards = selectedShards,
                    direction = direction,
                    pageLimit = pageLimit,
                    shardSelector = shardSelector,
                    totalMessages = totalMessages,
                    distanceFromLast = tailPageCursor,
                )
            } else {
                readMergedStreamMessages(group, selectedShards, direction, cursor, pageLimit, shardSelector, totalMessages)
            }
        }
    }

    private fun searchStreamMessagesByRecordId(
        group: GroupMetadata,
        shards: List<ShardId>,
        recordId: String,
        direction: StreamMessagePageDirection,
        pageLimit: Int,
        shardSelector: String,
        totalMessages: Long,
    ): List<GrafanaMessageRow> {
        val records = shards.flatMap { shard ->
            val streamKey = RedisStreamShardKeys.forShard(group.streamPrefix, shard.shardIndex).value
            redisCommands.xRange(streamKey, recordId, recordId, count = 1)
                .map { record ->
                    StreamMessageRecord(
                        streamPrefix = group.streamPrefix,
                        consumerGroup = group.consumerGroup,
                        shard = shard,
                        streamKey = streamKey,
                        recordId = record.id,
                        fields = record.fields,
                        payload = record.fields["payload"],
                    )
                }
        }.sortedBy { it.shard.shardIndex }
        val syntheticPage = StreamMessagesPageResponse(
            streamPrefix = group.streamPrefix,
            consumerGroup = group.consumerGroup,
            shard = records.firstOrNull()?.shard ?: shards.first(),
            streamKey = "SEARCH",
            direction = direction,
            limit = pageLimit,
            records = records,
            nextCursor = null,
        )
        return records.map { record ->
            record.toGrafanaMessageRow(
                page = syntheticPage,
                shardSelector = shardSelector,
                nextCursor = null,
                totalMessages = totalMessages,
            )
        }
    }

    private fun StreamMessageRecord.toGrafanaMessageRow(
        page: StreamMessagesPageResponse,
        shardSelector: String,
        nextCursor: String?,
        totalMessages: Long,
    ): GrafanaMessageRow =
        GrafanaMessageRow(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            shardIndex = shard.shardIndex,
            shardSelector = shardSelector,
            shardLabel = ":${shard.shardIndex}",
            streamKey = streamKey,
            recordId = recordId,
            payload = payload,
            fieldsJson = objectMapper.writeValueAsString(fields),
            pageDirection = page.direction,
            pageLimit = page.limit,
            pageNextCursor = nextCursor,
            pageTotalMessages = totalMessages,
        )

    private fun readMergedStreamMessages(
        group: GroupMetadata,
        shards: List<ShardId>,
        direction: StreamMessagePageDirection,
        cursor: String?,
        pageLimit: Int,
        shardSelector: String,
        totalMessages: Long,
    ): List<GrafanaMessageRow> {
        val cursorByShard = parseMergedMessageCursor(cursor)
        // pageLimit is the final response size. Each shard range uses a candidate
        // window so the coordinator can merge-sort shards and still return only
        // pageLimit rows to Grafana.
        val perShardCandidateLimit = (pageLimit + 1).toLong()
        val fetched = shards.flatMap { shard ->
            val streamKey = RedisStreamShardKeys.forShard(group.streamPrefix, shard.shardIndex).value
            val shardCursor = cursorByShard[shard.shardIndex]
            val records = when (direction) {
                StreamMessagePageDirection.FORWARD ->
                    redisCommands.xRange(streamKey, shardCursor ?: "-", "+", perShardCandidateLimit)
                StreamMessagePageDirection.BACKWARD ->
                    redisCommands.xRevRange(streamKey, shardCursor ?: "+", "-", perShardCandidateLimit)
            }.filterNot { record -> shardCursor != null && record.id == shardCursor }
            records.map { record ->
                StreamMessageRecord(
                    streamPrefix = group.streamPrefix,
                    consumerGroup = group.consumerGroup,
                    shard = shard,
                    streamKey = streamKey,
                    recordId = record.id,
                    fields = record.fields,
                    payload = record.fields["payload"],
                )
            }
        }
        val sorted = fetched.sortedWith { left, right ->
            compareRedisStreamIds(left.recordId, right.recordId).let { comparison ->
                val ordered = if (direction == StreamMessagePageDirection.FORWARD) comparison else -comparison
                if (ordered != 0) ordered else left.shard.shardIndex.compareTo(right.shard.shardIndex)
            }
        }
        val pageRecords = sorted.take(pageLimit)
        val nextCursor = if (sorted.size > pageLimit && pageRecords.isNotEmpty()) {
            encodeMergedMessageCursor(
                cursorByShard + pageRecords.associate { it.shard.shardIndex to it.recordId },
            )
        } else {
            null
        }
        val syntheticPage = StreamMessagesPageResponse(
            streamPrefix = group.streamPrefix,
            consumerGroup = group.consumerGroup,
            shard = pageRecords.firstOrNull()?.shard ?: shards.first(),
            streamKey = "ALL",
            direction = direction,
            limit = pageLimit,
            records = pageRecords,
            nextCursor = nextCursor,
        )
        return pageRecords.map { record ->
            record.toGrafanaMessageRow(
                page = syntheticPage,
                shardSelector = shardSelector,
                nextCursor = nextCursor,
                totalMessages = totalMessages,
            )
        }
    }

    private fun readTailMergedStreamMessages(
        group: GroupMetadata,
        shards: List<ShardId>,
        direction: StreamMessagePageDirection,
        pageLimit: Int,
        shardSelector: String,
        totalMessages: Long,
        distanceFromLast: Int,
    ): List<GrafanaMessageRow> {
        val window = tailMessagePageWindow(totalMessages, pageLimit, distanceFromLast)
        if (window.pageSize == 0) {
            return emptyList()
        }
        val readDirection = oppositeMessageDirection(direction)
        val readCount = window.readCount.toLong()
        val fetched = shards.flatMap { shard ->
            val streamKey = RedisStreamShardKeys.forShard(group.streamPrefix, shard.shardIndex).value
            val records = when (readDirection) {
                StreamMessagePageDirection.FORWARD ->
                    redisCommands.xRange(streamKey, "-", "+", readCount)
                StreamMessagePageDirection.BACKWARD ->
                    redisCommands.xRevRange(streamKey, "+", "-", readCount)
            }
            records.map { record ->
                StreamMessageRecord(
                    streamPrefix = group.streamPrefix,
                    consumerGroup = group.consumerGroup,
                    shard = shard,
                    streamKey = streamKey,
                    recordId = record.id,
                    fields = record.fields,
                    payload = record.fields["payload"],
                )
            }
        }
        val pageRecords = fetched
            .sortedWith(messageRecordComparator(readDirection))
            .take(window.readCount)
            .drop(window.offsetFromTail)
            .take(window.pageSize)
            .sortedWith(messageRecordComparator(direction))
        val syntheticPage = StreamMessagesPageResponse(
            streamPrefix = group.streamPrefix,
            consumerGroup = group.consumerGroup,
            shard = pageRecords.firstOrNull()?.shard ?: shards.first(),
            streamKey = "ALL",
            direction = direction,
            limit = pageLimit,
            records = pageRecords,
            nextCursor = tailNextCursor(window.distanceFromLast),
        )
        return pageRecords.map { record ->
            record.toGrafanaMessageRow(
                page = syntheticPage,
                shardSelector = shardSelector,
                nextCursor = syntheticPage.nextCursor,
                totalMessages = totalMessages,
            )
        }
    }

    private fun readStreamMessages(
        group: GroupMetadata,
        shardIndex: Int,
        direction: StreamMessagePageDirection,
        cursor: String?,
        limit: Int,
    ): StreamMessagesPageResponse {
        val shard = requireKnownShard(group, shardIndex)
        val streamKey = RedisStreamShardKeys.forShard(group.streamPrefix, shardIndex).value
        val pageLimit = normalizeMessagePageLimit(limit)
        val pageCursor = cursor?.takeIf { it.isNotBlank() }
        val fetchLimit = (pageLimit + if (pageCursor == null) 1 else 2).toLong()
        val fetchedRecords = when (direction) {
            StreamMessagePageDirection.FORWARD ->
                redisCommands.xRange(streamKey, pageCursor ?: "-", "+", fetchLimit)
            StreamMessagePageDirection.BACKWARD ->
                redisCommands.xRevRange(streamKey, pageCursor ?: "+", "-", fetchLimit)
        }.filterNot { record -> pageCursor != null && record.id == pageCursor }
        val records = fetchedRecords.take(pageLimit)
        return StreamMessagesPageResponse(
            streamPrefix = group.streamPrefix,
            consumerGroup = group.consumerGroup,
            shard = shard,
            streamKey = streamKey,
            direction = direction,
            limit = pageLimit,
            records = records.map { record ->
                StreamMessageRecord(
                    streamPrefix = group.streamPrefix,
                    consumerGroup = group.consumerGroup,
                    shard = shard,
                    streamKey = streamKey,
                    recordId = record.id,
                    fields = record.fields,
                    payload = record.fields["payload"],
                )
            },
            nextCursor = records.lastOrNull()?.id?.takeIf { fetchedRecords.size > pageLimit },
        )
    }

    private fun readTailStreamMessages(
        group: GroupMetadata,
        shardIndex: Int,
        direction: StreamMessagePageDirection,
        limit: Int,
        totalMessages: Long,
        distanceFromLast: Int,
    ): StreamMessagesPageResponse {
        val shard = requireKnownShard(group, shardIndex)
        val streamKey = RedisStreamShardKeys.forShard(group.streamPrefix, shardIndex).value
        val pageLimit = normalizeMessagePageLimit(limit)
        val window = tailMessagePageWindow(totalMessages, pageLimit, distanceFromLast)
        if (window.pageSize == 0) {
            return StreamMessagesPageResponse(
                streamPrefix = group.streamPrefix,
                consumerGroup = group.consumerGroup,
                shard = shard,
                streamKey = streamKey,
                direction = direction,
                limit = pageLimit,
                records = emptyList(),
                nextCursor = null,
            )
        }
        val readDirection = oppositeMessageDirection(direction)
        val records = when (direction) {
            StreamMessagePageDirection.FORWARD ->
                redisCommands.xRevRange(streamKey, "+", "-", window.readCount.toLong())
            StreamMessagePageDirection.BACKWARD ->
                redisCommands.xRange(streamKey, "-", "+", window.readCount.toLong())
        }.drop(window.offsetFromTail)
            .take(window.pageSize)
            .map { record ->
                StreamMessageRecord(
                    streamPrefix = group.streamPrefix,
                    consumerGroup = group.consumerGroup,
                    shard = shard,
                    streamKey = streamKey,
                    recordId = record.id,
                    fields = record.fields,
                    payload = record.fields["payload"],
                )
            }
            .sortedWith(messageRecordComparator(direction))
        return StreamMessagesPageResponse(
            streamPrefix = group.streamPrefix,
            consumerGroup = group.consumerGroup,
            shard = shard,
            streamKey = streamKey,
            direction = direction,
            limit = pageLimit,
            records = records,
            nextCursor = tailNextCursor(window.distanceFromLast),
        )
    }

    /**
     * Lists all resharding records for a group.
     */
    fun migrations(streamPrefix: String, consumerGroup: String): MigrationsResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        return MigrationsResponse(
            migrations = group.migrations.values.sortedBy { it.createdAt },
            activeReshardingId = group.activeReshardingId,
        )
    }

    private fun requireGroup(streamPrefix: String, consumerGroup: String): GroupMetadata =
        stateStore.get(GroupKey(streamPrefix, consumerGroup))
            ?: throw CoordinatorException(CoordinatorError.GROUP_NOT_FOUND)

    private fun requireKnownShard(group: GroupMetadata, shardIndex: Int): ShardId {
        require(shardIndex in 0 until group.shardCount) {
            "shardIndex must be between 0 and ${group.shardCount - 1}"
        }
        return ShardId(shardIndex)
    }

    private fun selectedMessageShards(group: GroupMetadata, shardSelector: String): List<ShardId> {
        if (shardSelector == ".*" || shardSelector.equals("all", ignoreCase = true) ||
            shardSelector == "__all" || shardSelector == "\$__all"
        ) {
            return (0 until group.shardCount).map { ShardId(it) }
        }
        return listOf(requireKnownShard(group, shardSelector.toInt()))
    }

    private fun normalizeMessagePageLimit(limit: Int): Int =
        limit.coerceIn(1, MAX_MESSAGE_PAGE_SIZE)

    private fun lastMessagePageSize(totalMessages: Long, pageLimit: Int): Int {
        if (totalMessages <= 0) {
            return 0
        }
        val remainder = (totalMessages % pageLimit).toInt()
        return if (remainder == 0) pageLimit else remainder
    }

    private fun tailMessagePageWindow(
        totalMessages: Long,
        pageLimit: Int,
        requestedDistanceFromLast: Int,
    ): TailMessagePageWindow {
        if (totalMessages <= 0) {
            return TailMessagePageWindow(
                distanceFromLast = 0,
                offsetFromTail = 0,
                pageSize = 0,
                readCount = 0,
            )
        }
        val pageCount = ((totalMessages + pageLimit - 1) / pageLimit).toInt()
        val lastPageIndex = pageCount - 1
        val targetPageIndex = (lastPageIndex - requestedDistanceFromLast.coerceAtLeast(0)).coerceAtLeast(0)
        val distanceFromLast = lastPageIndex - targetPageIndex
        val lastPageSize = lastMessagePageSize(totalMessages, pageLimit)
        val offsetFromTail = if (targetPageIndex == lastPageIndex) {
            0
        } else {
            lastPageSize + ((lastPageIndex - targetPageIndex - 1) * pageLimit)
        }
        val pageSize = if (targetPageIndex == lastPageIndex) lastPageSize else pageLimit
        return TailMessagePageWindow(
            distanceFromLast = distanceFromLast,
            offsetFromTail = offsetFromTail,
            pageSize = pageSize,
            readCount = offsetFromTail + pageSize,
        )
    }

    private data class TailMessagePageWindow(
        val distanceFromLast: Int,
        val offsetFromTail: Int,
        val pageSize: Int,
        val readCount: Int,
    )

    private fun oppositeMessageDirection(direction: StreamMessagePageDirection): StreamMessagePageDirection =
        when (direction) {
            StreamMessagePageDirection.FORWARD -> StreamMessagePageDirection.BACKWARD
            StreamMessagePageDirection.BACKWARD -> StreamMessagePageDirection.FORWARD
        }

    private fun messageRecordComparator(direction: StreamMessagePageDirection): Comparator<StreamMessageRecord> =
        Comparator { left, right ->
            compareRedisStreamIds(left.recordId, right.recordId).let { comparison ->
                val ordered = if (direction == StreamMessagePageDirection.FORWARD) comparison else -comparison
                if (ordered != 0) ordered else left.shard.shardIndex.compareTo(right.shard.shardIndex)
            }
        }

    private fun parseMergedMessageCursor(cursor: String?): Map<Int, String> =
        cursor?.takeIf { it.startsWith("all:") }
            ?.removePrefix("all:")
            ?.takeIf { it.isNotBlank() }
            ?.split(",")
            ?.mapNotNull { token ->
                val parts = token.split("=", limit = 2)
                parts.getOrNull(0)?.toIntOrNull()?.let { shardIndex ->
                    parts.getOrNull(1)?.takeIf { it.isNotBlank() }?.let { recordId -> shardIndex to recordId }
                }
            }
            ?.toMap()
            .orEmpty()

    private fun parseTailMessageCursor(cursor: String?): Int? {
        val value = cursor?.trim()?.takeIf { it.isNotEmpty() } ?: return null
        if (value == MESSAGE_LAST_PAGE_CURSOR) {
            return 0
        }
        return value
            .takeIf { it.startsWith(MESSAGE_TAIL_PAGE_CURSOR_PREFIX) }
            ?.removePrefix(MESSAGE_TAIL_PAGE_CURSOR_PREFIX)
            ?.toIntOrNull()
            ?.coerceAtLeast(0)
    }

    private fun tailNextCursor(distanceFromLast: Int): String? =
        if (distanceFromLast > 0) {
            "$MESSAGE_TAIL_PAGE_CURSOR_PREFIX${distanceFromLast - 1}"
        } else {
            null
        }

    private fun encodeMergedMessageCursor(cursorByShard: Map<Int, String>): String =
        cursorByShard.toSortedMap().entries.joinToString(prefix = "all:", separator = ",") { (shardIndex, recordId) ->
            "$shardIndex=$recordId"
        }

    private fun compareRedisStreamIds(left: String, right: String): Int {
        val leftParts = left.split("-", limit = 2)
        val rightParts = right.split("-", limit = 2)
        val leftMs = leftParts.getOrNull(0)?.toLongOrNull() ?: 0L
        val rightMs = rightParts.getOrNull(0)?.toLongOrNull() ?: 0L
        if (leftMs != rightMs) {
            return leftMs.compareTo(rightMs)
        }
        val leftSequence = leftParts.getOrNull(1)?.toLongOrNull() ?: 0L
        val rightSequence = rightParts.getOrNull(1)?.toLongOrNull() ?: 0L
        return leftSequence.compareTo(rightSequence)
    }

    private fun requireNoMetadataCorrection(group: GroupMetadata) {
        if (group.metadataCorrection != null) {
            throw CoordinatorException(CoordinatorError.METADATA_SYNC_IN_PROGRESS)
        }
    }

    /**
     * Refreshes one group and persists it only when operational state changed.
     */
    private fun refreshGroupOperationalState(key: GroupKey, now: Instant): Boolean {
        val group = stateStore.get(key) ?: return false
        if (!refreshOperationalState(group, now)) {
            return false
        }
        stateStore.save(key, group)
        return true
    }

    /**
     * Retries optimistic Redis store conflicts that can still happen after mutex lease churn.
     */
    private fun <T> withStateConflictRetry(operation: String, block: () -> T): T {
        var attempts = 0
        while (true) {
            try {
                return block()
            } catch (error: CoordinatorStateConflictException) {
                attempts += 1
                metrics.recordStateConflict(operation, attempts)
                if (attempts >= 3) {
                    throw CoordinatorException(CoordinatorError.STATE_VERSION_CONFLICT)
                }
            }
        }
    }

    private fun GroupMetadata.key(): GroupKey =
        GroupKey(streamPrefix, consumerGroup)

    private fun registerOrRejoinMember(
        group: GroupMetadata,
        memberId: String,
        request: HeartbeatRequest,
        now: Instant,
    ): MemberMetadata {
        val memberName = request.memberName ?: memberId
        val nextEpoch = group.groupEpoch.coerceAtLeast(1)
        val existing = group.members[memberId]
        val member = existing ?: MemberMetadata(
            memberId = memberId,
            memberName = memberName,
            state = MemberState.STARTING,
            memberEpoch = nextEpoch,
            metadataVersion = group.metadataVersion,
            runtimeMaxConcurrency = request.runtimeConsumerCapacity.runtimeMaxConcurrency,
            activeConsumerWorkers = 0,
            currentAssignment = emptySet(),
            revoking = emptySet(),
            lastHeartbeatAt = now,
            memberLeaseExpiresAt = now.plus(properties.memberLeaseTtl),
            rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
        ).also { group.members[memberId] = it }

        if (existing != null) {
            member.currentAssignment = emptySet()
            member.grantedAssignment = emptySet()
            member.revoking = emptySet()
            member.shardProgress = emptyList()
            member.rebalanceDeadlineAt = null
        }
        member.state = MemberState.ACTIVE
        member.memberEpoch = group.groupEpoch.coerceAtLeast(1)
        bumpMetadata(group, now, bumpGroupEpoch = true)
        return member
    }

    /**
     * Accepts only ownership reports that match previous ownership or coordinator-granted targets.
     */
    private fun validateOwnershipReport(
        group: GroupMetadata,
        member: MemberMetadata,
        request: HeartbeatRequest,
    ): ValidatedOwnershipReport? {
        return OwnershipReportValidator.validate(
            group = group,
            member = member,
            request = request,
            readableShards = group.readableShards().toSet(),
            blockedTargetShards = blockedShards(group, member.memberId),
        )
    }

    /**
     * Starts a group-wide request for consumers to discard a higher local metadata view and use Redis current state.
     */
    private fun startMetadataCorrection(group: GroupMetadata, observedMetadataVersion: Long, now: Instant) {
        val correction = group.metadataCorrection
        if (correction == null || correction.targetMetadataVersion != group.metadataVersion) {
            group.metadataCorrection = MetadataCorrection(
                targetMetadataVersion = group.metadataVersion,
                observedMetadataVersion = observedMetadataVersion,
                requestedAt = now,
                updatedAt = now,
            )
        } else {
            correction.observedMetadataVersion = maxOf(correction.observedMetadataVersion, observedMetadataVersion)
            correction.updatedAt = now
        }
        group.updatedAt = now
    }

    /**
     * Keeps member liveness current while rejecting stale ownership from a higher metadata version.
     */
    private fun refreshMemberLivenessForMetadataSync(
        group: GroupMetadata,
        member: MemberMetadata,
        request: HeartbeatRequest,
        now: Instant,
    ) {
        member.memberName = request.memberName ?: member.memberName
        member.metadataVersion = request.metadataVersion
        member.runtimeMaxConcurrency = request.runtimeConsumerCapacity.runtimeMaxConcurrency
        member.activeConsumerWorkers =
            (request.runtimeConsumerCapacity.runtimeMaxConcurrency - request.runtimeConsumerCapacity.availableConcurrency)
                .coerceAtLeast(0)
        member.rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis()
        member.lastHeartbeatAt = now
        member.memberLeaseExpiresAt = now.plus(properties.memberLeaseTtl)
        group.updatedAt = now
    }

    private fun refreshMemberLiveness(
        group: GroupMetadata,
        member: MemberMetadata,
        request: HeartbeatRequest,
        now: Instant,
    ) {
        member.memberName = request.memberName ?: member.memberName
        member.metadataVersion = group.metadataVersion
        member.runtimeMaxConcurrency = request.runtimeConsumerCapacity.runtimeMaxConcurrency
        member.activeConsumerWorkers =
            (request.runtimeConsumerCapacity.runtimeMaxConcurrency - request.runtimeConsumerCapacity.availableConcurrency)
                .coerceAtLeast(0)
        member.rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis()
        member.lastHeartbeatAt = now
        member.memberLeaseExpiresAt = now.plus(properties.memberLeaseTtl)
        group.updatedAt = now
    }

    /**
     * During metadata correction, stale revoking reports from a discarded future view are ignored, not accepted.
     */
    private fun metadataCorrectionAwareRequest(
        group: GroupMetadata,
        member: MemberMetadata,
        request: HeartbeatRequest,
    ): HeartbeatRequest {
        val correction = group.metadataCorrection ?: return request
        if (request.metadataVersion != correction.targetMetadataVersion) {
            return request
        }
        val authority = member.currentAssignment +
            member.revoking +
            group.targetAssignments[member.memberId].orEmpty()
        return request.copy(
            revokingShards = request.revokingShards.filter { it.shard in authority },
            shardProgress = request.shardProgress.filter { it.shard in authority },
        )
    }

    /**
     * Marks one consumer as corrected; the correction round ends after all live members report the target version.
     */
    private fun acknowledgeMetadataCorrectionIfNeeded(
        group: GroupMetadata,
        memberId: String,
        request: HeartbeatRequest,
        now: Instant,
    ) {
        val correction = group.metadataCorrection ?: return
        if (correction.targetMetadataVersion != group.metadataVersion) {
            group.metadataCorrection = null
            group.updatedAt = now
            return
        }
        if (request.metadataVersion == correction.targetMetadataVersion) {
            correction.acknowledgedMembers += memberId
            correction.updatedAt = now
            val liveMemberIds = group.metadataCorrectionParticipantMemberIds()
            correction.acknowledgedMembers.retainAll(liveMemberIds)
            if (liveMemberIds.isNotEmpty() &&
                correction.acknowledgedMembers.containsAll(liveMemberIds) &&
                !hasMetadataCorrectionHandoffPending(group)
            ) {
                group.metadataCorrection = null
            }
            group.updatedAt = now
        }
    }

    /**
     * Keeps the metadata correction round open until all previous owners have drained or been fenced.
     */
    private fun hasMetadataCorrectionHandoffPending(group: GroupMetadata): Boolean {
        val liveMembers = group.members.values
            .filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING || it.state == MemberState.LEAVING }
        return liveMembers.any { it.revoking.isNotEmpty() } ||
            group.targetAssignments.keys.any { blockedShards(group, it).isNotEmpty() }
    }

    /**
     * Fences a member that reports ownership for shards the coordinator did not grant.
     */
    private fun fenceInvalidOwnershipReport(
        group: GroupMetadata,
        member: MemberMetadata,
        request: HeartbeatRequest,
        now: Instant,
    ): HeartbeatResponse {
        member.state = MemberState.FENCED
        member.memberEpoch = (group.groupEpoch + 1).coerceAtLeast(member.memberEpoch + 1)
        member.currentAssignment = emptySet()
        member.grantedAssignment = emptySet()
        member.revoking = emptySet()
        member.rebalanceDeadlineAt = null
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        stateStore.save(group.key(), group)
        recordGroupState(group)
        return fencedHeartbeat(group, request, member.memberId, member)
    }

    /**
     * Applies all state-machine transitions that depend only on current time and stored metadata.
     */
    private fun refreshOperationalState(group: GroupMetadata, now: Instant): Boolean {
        val expired = expireMembers(group, now) > 0
        val timedOut = enforceRebalanceTimeouts(group, now)
        val migrationAdvanced = advanceMigrationDrainState(group, now)
        val pruned = pruneStaleMembers(group, now) > 0
        return expired || timedOut || migrationAdvanced || pruned
    }

    /**
     * Marks a member as leaving so its shards are drained before reassignment.
     */
    private fun markLeaving(
        group: GroupMetadata,
        memberId: String,
        request: HeartbeatRequest,
        now: Instant,
    ): MemberMetadata {
        val member = group.members[memberId] ?: return registerOrRejoinMember(group, memberId, request, now)
        if (member.state != MemberState.LEAVING) {
            member.state = MemberState.LEAVING
            member.memberEpoch = -1
            bumpMetadata(group, now, bumpGroupEpoch = true)
        }
        return member
    }

    private fun fencedHeartbeat(
        group: GroupMetadata,
        request: HeartbeatRequest,
        memberId: String,
        existing: MemberMetadata,
    ): HeartbeatResponse =
        HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.FENCED_MEMBER_EPOCH,
            memberId = memberId,
            memberEpoch = existing.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignment = AssignmentView(emptySet(), emptySet(), group.metadataVersion),
        )

    private fun metadataSyncHeartbeat(
        group: GroupMetadata,
        request: HeartbeatRequest,
        memberId: String,
        existing: MemberMetadata,
    ): HeartbeatResponse {
        val target = group.targetAssignments[memberId].orEmpty()
        val blocked = blockedShards(group, memberId)
        val assignable = target.filterNot { it in blocked }.toSet()
        val assigned = request.ownedShards
            .filter { it in assignable }
            .toSortedSet()
        val pending = (target - assigned).toSortedSet()
        existing.grantedAssignment = assigned
        return HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.SYNC_METADATA,
            memberId = memberId,
            memberEpoch = existing.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignment = AssignmentView(
                assignedShards = assigned,
                pendingShards = pending,
                metadataVersion = group.metadataVersion,
            ),
        )
    }

    private fun initialJoinReplayHeartbeat(
        group: GroupMetadata,
        request: HeartbeatRequest,
        existing: MemberMetadata,
    ): HeartbeatResponse {
        val target = group.targetAssignments[existing.memberId].orEmpty()
        val blocked = blockedShards(group, existing.memberId)
        val assigned = target.filterNot { it in blocked }.toSortedSet()
        val pending = target.filter { it in blocked }.toSortedSet()
        existing.grantedAssignment = assigned
        return HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.OK,
            memberId = existing.memberId,
            memberEpoch = existing.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignment = AssignmentView(
                assignedShards = assigned,
                pendingShards = pending,
                metadataVersion = group.metadataVersion,
            ),
        )
    }

    private fun revokePendingHeartbeat(
        group: GroupMetadata,
        request: HeartbeatRequest,
        member: MemberMetadata,
        target: Set<ShardId>,
        blocked: Set<ShardId>,
    ): HeartbeatResponse {
        val assignable = target.filterNot { it in blocked }.toSet()
        val assigned = member.currentAssignment
            .filter { it in assignable }
            .toSortedSet()
        val pending = (target - assigned).toSortedSet()
        member.grantedAssignment = assigned
        return HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.REVOKE_PENDING,
            memberId = member.memberId,
            memberEpoch = member.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignment = AssignmentView(
                assignedShards = assigned,
                pendingShards = pending,
                metadataVersion = group.metadataVersion,
            ),
        )
    }

    private fun rejectedHeartbeat(
        request: HeartbeatRequest,
        memberId: String,
        status: HeartbeatStatus,
    ): HeartbeatResponse =
        HeartbeatResponse(
            responseTo = request.requestId,
            status = status,
            memberId = memberId,
            memberEpoch = request.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
            groupEpoch = 0,
            assignmentEpoch = 0,
            metadataVersion = 0,
            assignment = AssignmentView(emptySet(), emptySet(), 0),
        )

    /**
     * Expires members that missed their lease deadline and forces a rebalance.
     */
    private fun expireMembers(group: GroupMetadata, now: Instant): Int {
        var expiredCount = 0
        group.members.values.forEach { member ->
            if ((member.state == MemberState.ACTIVE || member.state == MemberState.STARTING) && now.isAfter(member.memberLeaseExpiresAt)) {
                member.state = MemberState.EXPIRED
                member.memberEpoch = member.memberEpoch + 1
                expiredCount += 1
            }
        }
        if (expiredCount > 0) {
            bumpMetadata(group, now, bumpGroupEpoch = true)
            reconcile(group, now)
            metrics.recordMemberExpired(group, expiredCount)
        }
        return expiredCount
    }

    /**
     * Removes terminal member records after a retention window so in-memory metadata cannot grow without bound.
     */
    private fun pruneStaleMembers(group: GroupMetadata, now: Instant): Int {
        val retention = properties.staleMemberRetention
        if (retention.isZero || retention.isNegative) {
            return 0
        }

        val staleMemberIds = group.members.values
            .filter { member -> member.isPrunable(now, retention) }
            .map { it.memberId }
        if (staleMemberIds.isEmpty()) {
            return 0
        }

        staleMemberIds.forEach { memberId ->
            group.members.remove(memberId)
            group.targetAssignments.remove(memberId)
            group.metadataCorrection?.acknowledgedMembers?.remove(memberId)
        }
        reconcileMetadataCorrectionAfterPrune(group, now)
        bumpMetadata(group, now, bumpGroupEpoch = false)
        return staleMemberIds.size
    }

    /**
     * Fences members that do not revoke removed shards before their rebalance deadline.
     */
    private fun enforceRebalanceTimeouts(group: GroupMetadata, now: Instant): Boolean {
        var changed = false
        var fenced = false
        group.members.values
            .filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING || it.state == MemberState.LEAVING }
            .forEach { member ->
                val target = group.targetAssignments[member.memberId].orEmpty()
                val shardsToRelease = (member.currentAssignment + member.revoking) - target
                if (shardsToRelease.isEmpty()) {
                    if (member.rebalanceDeadlineAt != null) {
                        member.rebalanceDeadlineAt = null
                        changed = true
                    }
                    return@forEach
                }

                val deadline = member.rebalanceDeadlineAt
                if (deadline == null) {
                    member.rebalanceDeadlineAt = now.plusMillis(member.rebalanceTimeoutMs.coerceAtLeast(1))
                    changed = true
                } else if (!now.isBefore(deadline)) {
                    member.state = MemberState.FENCED
                    member.memberEpoch = (group.groupEpoch + 1).coerceAtLeast(member.memberEpoch + 1)
                    member.currentAssignment = emptySet()
                    member.grantedAssignment = emptySet()
                    member.revoking = emptySet()
                    member.rebalanceDeadlineAt = null
                    changed = true
                    fenced = true
                }
            }

        if (fenced) {
            bumpMetadata(group, now, bumpGroupEpoch = true)
            reconcile(group, now)
        }
        return changed
    }

    /**
     * Advances resharding from ACTIVE to DRAINING to DEPRECATED as consumers converge.
     */
    private fun advanceMigrationDrainState(group: GroupMetadata, now: Instant): Boolean {
        val migration = group.activeReshardingId?.let { group.migrations[it] } ?: return false
        return when (migration.state) {
            MigrationState.ACTIVE -> startMigrationDrainIfReady(group, migration, now)
            MigrationState.DRAINING -> completeMigrationDrainIfReady(group, migration, now)
            else -> false
        }
    }

    /**
     * Starts draining removed shards once all live members converged on the target assignment.
     */
    private fun startMigrationDrainIfReady(
        group: GroupMetadata,
        migration: Migration,
        now: Instant,
    ): Boolean {
        val liveMembers = group.liveMembers()
        if (liveMembers.isEmpty() || group.state != GroupState.STABLE) {
            return false
        }
        if (!liveMembers.all { member -> member.currentAssignment == group.targetAssignments[member.memberId].orEmpty() }) {
            return false
        }

        migration.state = MigrationState.DRAINING
        migration.updatedAt = now
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        return true
    }

    /**
     * Completes resharding after no live member still owns or revokes removed shards.
     */
    private fun completeMigrationDrainIfReady(
        group: GroupMetadata,
        migration: Migration,
        now: Instant,
    ): Boolean {
        val removedShardStillOwned = group.liveMembers().any { member ->
            member.currentAssignment.any { it.shardIndex >= migration.toShardCount } ||
                member.revoking.any { it.shardIndex >= migration.toShardCount }
        }
        if (removedShardStillOwned) {
            return false
        }

        migration.state = MigrationState.DEPRECATED
        migration.updatedAt = now
        group.activeReshardingId = null
        bumpMetadata(group, now, bumpGroupEpoch = false)
        reconcile(group, now)
        return true
    }

    /**
     * Computes the target assignment and group state from live members and readable shards.
     */
    private fun reconcile(group: GroupMetadata, now: Instant) {
        val startedAt = Instant.now(clock)
        val previousAssignments = group.targetAssignmentSnapshot()
        val previousState = group.state
        val liveMembers = group.liveMembers()
            .sortedBy { it.memberId }

        if (liveMembers.isEmpty()) {
            group.targetAssignments.clear()
            group.assignmentEpoch = group.groupEpoch
            group.state = GroupState.EMPTY
            group.updatedAt = now
            recordRebalanceIfChanged(group, previousAssignments, previousState, startedAt)
            return
        }

        group.state = GroupState.ASSIGNING
        group.targetAssignments = computeStickyAssignment(group, liveMembers)
        group.assignmentEpoch = group.groupEpoch
        group.state = if (hasPendingConvergence(group)) GroupState.RECONCILING else GroupState.STABLE
        group.updatedAt = now
        recordRebalanceIfChanged(group, previousAssignments, previousState, startedAt)
    }

    /**
     * Records rebalance metrics only when target assignments or group state changed.
     */
    private fun recordRebalanceIfChanged(
        group: GroupMetadata,
        previousAssignments: Map<String, Set<ShardId>>,
        previousState: GroupState,
        startedAt: Instant,
    ) {
        val changed = previousAssignments != group.targetAssignmentSnapshot() || previousState != group.state
        if (changed) {
            metrics.recordRebalance(group, "reconcile", Duration.between(startedAt, Instant.now(clock)))
        }
        recordGroupState(group)
    }

    /**
     * Preserves previous owners where possible, then fills and balances remaining shard ownership.
     */
    private fun computeStickyAssignment(
        group: GroupMetadata,
        liveMembers: List<MemberMetadata>,
    ): MutableMap<String, MutableSet<ShardId>> {
        val liveIds = liveMembers.map { it.memberId }.toSet()
        val result = liveIds.associateWith { mutableSetOf<ShardId>() }.toMutableMap()
        val allShards = group.readableShards()
        val unassigned = linkedSetOf<ShardId>()

        allShards.forEach { shard ->
            val previousOwner = group.targetAssignments.entries
                .firstOrNull { (memberId, shards) -> memberId in liveIds && shard in shards }
                ?.key
            if (previousOwner != null) {
                result.getValue(previousOwner).add(shard)
            } else {
                unassigned.add(shard)
            }
        }

        unassigned.sorted().forEach { shard ->
            val owner = liveMembers.minWith(compareBy<MemberMetadata> {
                result.getValue(it.memberId).size
            }.thenBy { it.memberId })
            result.getValue(owner.memberId).add(shard)
        }

        balanceExistingAssignments(result)

        return result
    }

    /**
     * Moves shards from overloaded members only when the shard-count spread improves.
     */
    private fun balanceExistingAssignments(
        assignments: MutableMap<String, MutableSet<ShardId>>,
    ) {
        if (assignments.size <= 1) return

        while (true) {
            val currentSpread = assignmentSpread(assignments)
            val mostLoaded = assignments.maxWith(memberLoadComparator().thenByDescending { it.key })
            val leastLoaded = assignments.minWith(memberLoadComparator().thenBy { it.key })
            val movedShard = mostLoaded.value.sortedDescending().firstOrNull { shard ->
                mostLoaded.value.remove(shard)
                leastLoaded.value.add(shard)
                val improves = assignmentSpread(assignments) < currentSpread
                leastLoaded.value.remove(shard)
                mostLoaded.value.add(shard)
                improves
            } ?: return

            mostLoaded.value.remove(movedShard)
            leastLoaded.value.add(movedShard)
        }
    }

    private fun memberLoadComparator(): Comparator<Map.Entry<String, MutableSet<ShardId>>> =
        compareBy { entry -> entry.value.size }

    private fun assignmentSpread(assignments: Map<String, Set<ShardId>>): Int {
        val loads = assignments.map { (_, shards) -> shards.size }
        return (loads.maxOrNull() ?: 0) - (loads.minOrNull() ?: 0)
    }

    /**
     * Finds target shards that cannot be assigned until another member reports revoke completion.
     */
    private fun blockedShards(group: GroupMetadata, targetMemberId: String): Set<ShardId> {
        val target = group.targetAssignments[targetMemberId].orEmpty()
        if (target.isEmpty()) return emptySet()
        return target.filter { shard ->
            group.members.values.any { member ->
                member.memberId != targetMemberId &&
                    (member.state == MemberState.ACTIVE || member.state == MemberState.LEAVING || member.state == MemberState.STARTING) &&
                    (shard in member.currentAssignment || shard in member.revoking)
            }
        }.toSet()
    }

    /**
     * Checks whether current ownership has caught up with the coordinator target assignment.
     */
    private fun hasPendingConvergence(group: GroupMetadata): Boolean =
        group.targetAssignments.any { (memberId, target) ->
            val member = group.members[memberId] ?: return@any true
            val blocked = blockedShards(group, memberId)
            member.currentAssignment != target - blocked || blocked.isNotEmpty()
        }

    /**
     * Increments metadataVersion for every state change and groupEpoch for assignment-affecting changes.
     */
    private fun bumpMetadata(group: GroupMetadata, now: Instant, bumpGroupEpoch: Boolean) {
        if (bumpGroupEpoch) {
            group.groupEpoch += 1
        }
        group.metadataVersion += 1
        group.updatedAt = now
    }

    private fun recordGroupState(group: GroupMetadata) {
        metrics.recordGroupState(group, invariantViolations(group).size)
    }

    private fun GroupMetadata.targetAssignmentSnapshot(): Map<String, Set<ShardId>> =
        targetAssignments
            .mapValues { (_, shards) -> shards.toSortedSet() }
            .toSortedMap()

    private fun GroupMetadata.readableShards(): List<ShardId> =
        (0 until shardCount).map { ShardId(it) }.sorted()

    private fun GroupMetadata.liveMembers(): List<MemberMetadata> =
        members.values.filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }

    private fun MemberMetadata.isPrunable(now: Instant, retention: Duration): Boolean {
        val terminal = when (state) {
            MemberState.EXPIRED, MemberState.FENCED -> true
            MemberState.LEAVING -> currentAssignment.isEmpty() && revoking.isEmpty()
            MemberState.STARTING, MemberState.ACTIVE -> false
        }
        return terminal && !now.isBefore(lastHeartbeatAt.plus(retention))
    }

    private fun GroupMetadata.metadataCorrectionParticipantMemberIds(): Set<String> =
        members.values
            .filter { member ->
                member.state == MemberState.ACTIVE ||
                    member.state == MemberState.STARTING ||
                    (member.state == MemberState.LEAVING && (member.currentAssignment.isNotEmpty() || member.revoking.isNotEmpty()))
            }
            .map { it.memberId }
            .toSet()

    private fun reconcileMetadataCorrectionAfterPrune(group: GroupMetadata, now: Instant) {
        val correction = group.metadataCorrection ?: return
        val liveMemberIds = group.metadataCorrectionParticipantMemberIds()
        correction.acknowledgedMembers.retainAll(liveMemberIds)
        if (liveMemberIds.isEmpty() ||
            (correction.acknowledgedMembers.containsAll(liveMemberIds) && !hasMetadataCorrectionHandoffPending(group))
        ) {
            group.metadataCorrection = null
            group.updatedAt = now
        }
    }

    private fun GroupMetadata.toResponse(): GroupResponse =
        GroupResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            state = state,
            groupEpoch = groupEpoch,
            assignmentEpoch = assignmentEpoch,
            metadataVersion = metadataVersion,
            shardCount = shardCount,
            activeMigration = activeReshardingId?.let { migrations[it] },
            targetAssignmentSummary = targetAssignments.mapValues { it.value.size },
            currentAssignmentSummary = members.values
                .filter { it.isLiveOwner() }
                .filter { it.currentAssignment.isNotEmpty() }
                .associate { it.memberId to it.currentAssignment.size },
        )

    /**
     * Flattens member progress into a monitoring view keyed by member and shard.
     */
    private fun GroupMetadata.toConsumerShardProgress(): List<ConsumerShardProgress> =
        members.values
            .flatMap { member ->
                member.shardProgress.map { progress ->
                    ConsumerShardProgress(
                        streamPrefix = streamPrefix,
                        consumerGroup = consumerGroup,
                        memberId = member.memberId,
                        memberName = member.memberName,
                        memberState = member.state,
                        shard = progress.shard,
                        streamKey = progress.streamKey,
                        lastDeliveredId = progress.lastDeliveredId,
                        lastAckedId = progress.lastAckedId,
                        pendingCount = progress.pendingCount,
                        updatedAt = progress.updatedAt,
                    )
                }
            }
            .sortedWith(
                compareBy<ConsumerShardProgress> { it.memberId }
                    .thenBy { it.shard.shardIndex },
            )

    private fun GroupMetadata.toStreamShardOffsets(): StreamShardOffsetsResponse {
        val consumerProgress = toConsumerShardProgress()
            .groupBy { it.shard }
            .mapValues { (_, progress) ->
                progress.maxByOrNull { it.updatedAt ?: Instant.EPOCH }
            }
        val currentOwners = members.values
            .filter { it.isLiveOwner() }
            .flatMap { member -> member.currentAssignment.map { shard -> shard to member.memberId } }
            .groupBy({ it.first }, { it.second })
        val shardKeys = readableShards().associateWith { shard ->
            RedisStreamShardKeys.forShard(streamPrefix, shard.shardIndex)
        }
        val slotOwners = redisCommands.clusterSlotOwners(shardKeys.values.map { it.slot })
        val shards = shardKeys.map { (shard, shardKey) ->
            CompletableFuture.supplyAsync({
                monitoringShardPermits.withPermit {
                    val streamKey = shardKey.value
                    val streamInfo = redisCommands.xInfoStream(streamKey)
                    val groupInfo = redisCommands.xInfoGroups(streamKey).firstOrNull { it.name == consumerGroup }
                    val memoryUsageBytes = redisCommands.memoryUsage(streamKey)
                    val progress = consumerProgress[shard]
                    val slotOwner = slotOwners[shardKey.slot]
                    StreamShardOffset(
                        streamPrefix = streamPrefix,
                        consumerGroup = consumerGroup,
                        shard = shard,
                        streamKey = streamKey,
                        redisSlot = shardKey.slot,
                        redisNodeEndpoint = slotOwner?.endpoint,
                        redisNodeId = slotOwner?.nodeId,
                        redisSlotRangeStart = slotOwner?.slotRangeStart,
                        redisSlotRangeEnd = slotOwner?.slotRangeEnd,
                        streamLength = streamInfo.length,
                        firstRecordId = streamInfo.firstEntryId,
                        lastRecordId = streamInfo.lastEntryId,
                        lastGeneratedId = streamInfo.lastGeneratedId,
                        groupLastDeliveredId = groupInfo?.lastDeliveredId,
                        consumerLastDeliveredId = progress?.lastDeliveredId,
                        consumerLastAckedId = progress?.lastAckedId,
                        pendingCount = groupInfo?.pending ?: progress?.pendingCount ?: 0L,
                        lag = groupInfo?.lag,
                        memoryUsageBytes = memoryUsageBytes,
                        ownerMemberIds = currentOwners[shard].orEmpty().sorted(),
                    )
                }
            }, monitoringExecutor)
        }.map { it.get() }
        val totalMemoryUsageBytes = shards.mapNotNull { it.memoryUsageBytes }.sum()
        return StreamShardOffsetsResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            shards = shards,
            totalStreamLength = shards.sumOf { it.streamLength },
            totalPendingCount = shards.sumOf { it.pendingCount },
            totalLag = shards.map { it.lag }.takeIf { values -> values.all { it != null } }?.sumOf { it ?: 0L },
            totalMemoryUsageBytes = totalMemoryUsageBytes,
            memoryUsageKnown = shards.all { it.memoryUsageBytes != null },
        )
    }

    private fun GroupMetadata.toCachedStreamShardOffsets(): StreamShardOffsetsResponse {
        val cacheTtl = properties.monitoring.offsetCacheTtl
        if (cacheTtl.isZero || cacheTtl.isNegative) {
            return toStreamShardOffsets()
        }
        val now = Instant.now(clock)
        val key = MonitoringOffsetCacheKey(streamPrefix, consumerGroup)
        val cached = monitoringOffsetCache[key]
        if (cached != null && cached.shardCount == shardCount) {
            if (now.isBefore(cached.expiresAt)) {
                return cached.offsets
            }
            refreshMonitoringOffsetCacheAsync(key, cacheTtl)
            return cached.offsets
        }
        val lock = monitoringOffsetLocks.computeIfAbsent(key) { Any() }
        return synchronized(lock) {
            val refreshedNow = Instant.now(clock)
            val refreshedCached = monitoringOffsetCache[key]
            if (refreshedCached != null && refreshedCached.shardCount == shardCount) {
                if (refreshedNow.isBefore(refreshedCached.expiresAt)) {
                    refreshedCached.offsets
                } else {
                    refreshMonitoringOffsetCacheAsync(key, cacheTtl)
                    refreshedCached.offsets
                }
            } else {
                toStreamShardOffsets().also { offsets ->
                    monitoringOffsetCache[key] = MonitoringOffsetCacheEntry(
                        shardCount = shardCount,
                        expiresAt = refreshedNow.plus(cacheTtl),
                        offsets = offsets,
                    )
                }
            }
        }
    }

    private fun GroupMetadata.refreshMonitoringOffsetCacheAsync(
        key: MonitoringOffsetCacheKey,
        cacheTtl: Duration,
    ) {
        val refreshFlag = monitoringOffsetRefreshInFlight.computeIfAbsent(key) { AtomicBoolean(false) }
        if (!refreshFlag.compareAndSet(false, true)) {
            return
        }
        CompletableFuture.runAsync({
            try {
                val offsets = toStreamShardOffsets()
                monitoringOffsetCache[key] = MonitoringOffsetCacheEntry(
                    shardCount = shardCount,
                    expiresAt = Instant.now(clock).plus(cacheTtl),
                    offsets = offsets,
                )
            } finally {
                refreshFlag.set(false)
            }
        }, monitoringGroupExecutor)
    }

    private fun <T> Semaphore.withPermit(block: () -> T): T {
        acquire()
        try {
            return block()
        } finally {
            release()
        }
    }

    private fun MemberMetadata.isLiveOwner(): Boolean =
        state == MemberState.ACTIVE || state == MemberState.STARTING || state == MemberState.LEAVING

    /**
     * Converts the current shard layout into metadata consumed by producer-side routers.
     */
    private fun GroupMetadata.toProducerRoutingResponse(): ProducerRoutingResponse {
        val activeShardKeys = streamShardKeys()
        return ProducerRoutingResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            metadataVersion = metadataVersion,
            shardCount = shardCount,
            streamKeyPattern = RedisStreamShardKeys.keyPattern(streamPrefix),
            shards = activeShardKeys.map { shardKey ->
                ProducerRoutingShard(
                    shardIndex = shardKey.shardIndex,
                    streamKey = shardKey.value,
                    redisSlot = shardKey.slot,
                )
            },
        )
    }

    /**
     * Detects assignment consistency problems that should never appear in a healthy group.
     */
    private fun invariantViolations(group: GroupMetadata): List<String> {
        val violations = mutableListOf<String>()
        val owners = mutableMapOf<ShardId, MutableList<String>>()
        group.targetAssignments.forEach { (memberId, shards) ->
            if (memberId !in group.members) {
                violations += "target assignment references unknown member $memberId"
            }
            shards.forEach { owners.getOrPut(it) { mutableListOf() }.add(memberId) }
        }
        group.readableShards().forEach { shard ->
            val shardOwners = owners[shard].orEmpty()
            if (shardOwners.isEmpty() && group.members.values.any { it.state == MemberState.ACTIVE }) {
                violations += "readable shard $shard has no target owner"
            }
            if (shardOwners.size > 1) {
                violations += "readable shard $shard has duplicate target owners $shardOwners"
            }
        }
        if (group.assignmentEpoch != group.groupEpoch && group.state != GroupState.EMPTY) {
            violations += "assignmentEpoch ${group.assignmentEpoch} does not match groupEpoch ${group.groupEpoch}"
        }
        return violations
    }
}
