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
import java.time.Clock
import java.time.Duration
import java.time.Instant

data class CoordinatorTickResult(
    val scannedGroups: Int,
    val changedGroups: Int,
)

@Service
class CoordinatorService(
    private val properties: CoordinatorProperties,
    private val stateStore: CoordinatorStateStore,
    private val redisConnectionFactory: ObjectProvider<RedisConnectionFactory>,
    private val streamProvisioner: StreamShardProvisioner = NoopStreamShardProvisioner,
    private val clock: Clock = Clock.systemUTC(),
    private val metrics: CoordinatorMetrics = NoopCoordinatorMetrics,
    private val stateMutex: CoordinatorStateMutex = LocalCoordinatorStateMutex,
) {
    /**
     * Creates a coordinator group and provisions the initial stream shard version.
     */
    @CriticalSection(operation = "create-group")
    fun createGroup(streamPrefix: String, consumerGroup: String, request: CreateGroupRequest): GroupResponse =
        createGroupOnce(streamPrefix, consumerGroup, request)

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
        val policy = request.consumerConcurrencyPolicy
            ?: ConsumerConcurrencyPolicy(properties.defaults.consumerMaxConcurrency)
        val group = GroupMetadata(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            groupEpoch = 1,
            metadataVersion = 1,
            assignmentEpoch = 0,
            state = GroupState.EMPTY,
            activeWriteVersion = 1,
            readableVersions = setOf(1),
            shardCountsByVersion = linkedMapOf(1 to shardCount),
            consumerConcurrencyPolicy = policy,
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

    /**
     * Returns the current group view after applying time-based operational transitions.
     */
    @CriticalSection(operation = "get-group")
    fun getGroup(streamPrefix: String, consumerGroup: String): GroupResponse =
        withStateConflictRetry("get-group") {
            val group = requireGroup(streamPrefix, consumerGroup)
            if (refreshOperationalState(group, Instant.now(clock))) {
                stateStore.save(group.key(), group)
            }
            recordGroupState(group)
            group.toResponse()
        }

    /**
     * Returns producer routing metadata for the current active write stream version.
     */
    @CriticalSection(operation = "producer-routing")
    fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse {
        try {
            val response = withStateConflictRetry("producer-routing") {
                val group = requireGroup(streamPrefix, consumerGroup)
                if (refreshOperationalState(group, Instant.now(clock))) {
                    stateStore.save(group.key(), group)
                }
                recordGroupState(group)
                group.toProducerRoutingResponse()
            }
            metrics.recordProducerRouting(streamPrefix, consumerGroup, "SUCCESS")
            return response
        } catch (error: RuntimeException) {
            metrics.recordProducerRouting(streamPrefix, consumerGroup, "ERROR")
            throw error
        }
    }

    /**
     * Starts or resumes a shard-count change and moves producer writes to the new stream version.
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

        val fromVersion = group.activeWriteVersion
        val fromShardCount = group.shardCountsByVersion.getValue(fromVersion)
        if (fromShardCount == request.targetShardCount) {
            stateStore.save(group.key(), group)
            return Migration(
                reshardingId = "noop",
                fromVersion = fromVersion,
                toVersion = fromVersion,
                fromShardCount = fromShardCount,
                toShardCount = fromShardCount,
                state = MigrationState.DEPRECATED,
                createdAt = now,
                updatedAt = now,
            )
        }

        val toVersion = group.shardCountsByVersion.keys.maxOrNull()!! + 1
        val migration = Migration(
            reshardingId = newReshardingId(),
            fromVersion = fromVersion,
            toVersion = toVersion,
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
     * Provisions the target stream version, switches producer routing, and reconciles consumers.
     */
    private fun provisionAndActivatePreparedMigration(
        group: GroupMetadata,
        migration: Migration,
        request: ScaleGroupRequest,
        now: Instant,
    ): Migration {
        // Commit PREPARING first; conflict retries must not create Redis shards without matching coordinator state.
        streamProvisioner.provision(
            RedisStreamShardProvisioningPlan.forVersion(
                streamPrefix = group.streamPrefix,
                consumerGroup = group.consumerGroup,
                streamVersion = migration.toVersion,
                shardCount = migration.toShardCount,
            ),
        )

        migration.state = MigrationState.ACTIVE
        migration.updatedAt = now
        group.shardCountsByVersion[migration.toVersion] = migration.toShardCount
        group.activeWriteVersion = migration.toVersion
        group.readableVersions = setOf(migration.fromVersion, migration.toVersion)
        request.consumerConcurrencyPolicy?.let { group.consumerConcurrencyPolicy = it }
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        enforceRebalanceTimeouts(group, now)
        stateStore.save(group.key(), group)
        recordGroupState(group)
        return migration
    }

    /**
     * Updates member concurrency weights and triggers reassignment when effective capacity changes.
     */
    @CriticalSection(operation = "update-consumer-concurrency")
    fun updateConsumerConcurrency(
        streamPrefix: String,
        consumerGroup: String,
        request: UpdateConsumerConcurrencyRequest,
    ): ConsumerConcurrencyResponse {
        try {
            val response = withStateConflictRetry("update-consumer-concurrency") {
                updateConsumerConcurrencyOnce(streamPrefix, consumerGroup, request)
            }
            metrics.recordConsumerConcurrencyUpdate(streamPrefix, consumerGroup, "SUCCESS")
            return response
        } catch (error: RuntimeException) {
            metrics.recordConsumerConcurrencyUpdate(streamPrefix, consumerGroup, "ERROR")
            throw error
        }
    }

    /**
     * Applies a concurrency policy update inside the already-held state mutex.
     */
    private fun updateConsumerConcurrencyOnce(
        streamPrefix: String,
        consumerGroup: String,
        request: UpdateConsumerConcurrencyRequest,
    ): ConsumerConcurrencyResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        requireNoMetadataCorrection(group)
        val nextPolicy = ConsumerConcurrencyPolicy(request.defaultMaxConcurrency, request.memberOverrides)
        if (group.consumerConcurrencyPolicy != nextPolicy) {
            val now = Instant.now(clock)
            val previousTargetAssignments = group.targetAssignmentSnapshot()
            group.consumerConcurrencyPolicy = nextPolicy
            group.members.values.forEach { it.assignedMaxConcurrency = group.assignedMaxConcurrency(it.memberName) }
            reconcile(group, now)
            if (group.targetAssignmentSnapshot() != previousTargetAssignments) {
                bumpMetadata(group, now, bumpGroupEpoch = true)
                reconcile(group, now)
            } else {
                bumpMetadata(group, now, bumpGroupEpoch = false)
            }
            enforceRebalanceTimeouts(group, now)
            stateStore.save(group.key(), group)
        }
        recordGroupState(group)

        return ConsumerConcurrencyResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            metadataVersion = group.metadataVersion,
            groupEpoch = group.groupEpoch,
            consumerConcurrencyPolicy = group.consumerConcurrencyPolicy,
            affectedMembers = group.members.values
                .filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }
                .map { it.memberId },
        )
    }

    /**
     * Returns one resharding record by id.
     */
    @CriticalSection(operation = "get-migration")
    fun getMigration(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration =
        requireGroup(streamPrefix, consumerGroup).migrations[reshardingId]
            ?: throw CoordinatorException(CoordinatorError.MIGRATION_NOT_FOUND)

    /**
     * Rolls back an active resharding operation before the old version has been deprecated.
     */
    @CriticalSection(operation = "rollback-migration")
    fun rollbackMigration(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration =
        withStateConflictRetry("rollback-migration") { rollbackMigrationOnce(streamPrefix, consumerGroup, reshardingId) }

    /**
     * Restores active writes and reads to the source stream version.
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
        group.activeWriteVersion = migration.fromVersion
        group.readableVersions = setOf(migration.fromVersion)
        group.shardCountsByVersion.remove(migration.toVersion)
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
        val existing = group.members[memberId]
        val metadataSyncMember = existing
            ?.takeUnless { it.state == MemberState.FENCED || it.state == MemberState.EXPIRED }
        if (request.metadataVersion > group.metadataVersion && metadataSyncMember != null) {
            startMetadataCorrection(group, request.metadataVersion, now)
            refreshMemberLivenessForMetadataSync(group, metadataSyncMember, request, now)
            if (membersExpired > 0) {
                reconcile(group, now)
            }
            stateStore.save(group.key(), group)
            recordGroupState(group)
            return metadataSyncHeartbeat(group, request, memberId, metadataSyncMember)
        }
        if (group.metadataCorrection != null && metadataSyncMember != null && request.metadataVersion != group.metadataVersion) {
            refreshMemberLivenessForMetadataSync(group, metadataSyncMember, request, now)
            if (membersExpired > 0) {
                reconcile(group, now)
            }
            stateStore.save(group.key(), group)
            recordGroupState(group)
            return metadataSyncHeartbeat(group, request, memberId, metadataSyncMember)
        }
        val member = when {
            request.memberEpoch < -1L -> {
                if (membersExpired > 0) {
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
            request.memberEpoch == 0L -> {
                if (membersExpired > 0) {
                    stateStore.save(group.key(), group)
                }
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.INVALID_REQUEST)
            }
            existing.state == MemberState.FENCED || existing.state == MemberState.EXPIRED -> {
                if (membersExpired > 0) {
                    stateStore.save(group.key(), group)
                }
                return fencedHeartbeat(group, request, memberId, existing)
            }
            request.memberEpoch == -1L -> markLeaving(group, memberId, request, now)
            request.memberEpoch > existing.memberEpoch -> {
                if (membersExpired > 0) {
                    stateStore.save(group.key(), group)
                }
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.INVALID_REQUEST)
            }
            request.memberEpoch < existing.memberEpoch -> {
                if (membersExpired > 0) {
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
        member.assignedMaxConcurrency = group.assignedMaxConcurrency(member.memberName)
        member.rebalanceTimeoutMs = validatingRequest.rebalanceTimeoutMs ?: member.rebalanceTimeoutMs
        member.currentAssignment = if (member.state == MemberState.LEAVING) emptySet() else ownershipReport.ownedShards
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
        stateStore.save(group.key(), group)
        recordGroupState(group)

        val target = group.targetAssignments[memberId].orEmpty()
        val blocked = blockedShards(group, memberId)
        val assigned = target.filterNot { it in blocked }.toSortedSet()
        val pending = target.filter { it in blocked }.toSortedSet()
        if (group.metadataCorrection != null) {
            return revokePendingHeartbeat(group, request, member, target, blocked)
        }

        return HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.OK,
            memberId = memberId,
            memberEpoch = member.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignedMaxConcurrency = member.assignedMaxConcurrency,
            assignment = AssignmentView(
                assignedShards = assigned,
                pendingShards = pending,
                metadataVersion = group.metadataVersion,
            ),
        )
    }

    /**
     * Reports coordinator liveness and Redis dependency status.
     */
    fun health(): HealthResponse {
        val redisStatus = if (requiresRedis()) {
            redisConnectionFactory.ifAvailable?.let { factory ->
                runCatching {
                    CoordinatorRedisCommands(redisConnectionFactory = factory).ping()
                }.fold(
                    onSuccess = { "UP" },
                    onFailure = { "DOWN" },
                )
            } ?: "NOT_CONFIGURED"
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

    /**
     * Returns module-defined protocol compatibility metadata for operators and client diagnostics.
     */
    fun compatibility(): CoordinatorCompatibilityResponse =
        CoordinatorProtocol.compatibility()

    private fun requiresRedis(): Boolean =
        properties.store.type == CoordinatorProperties.StoreType.REDIS ||
            properties.streams.provisioningEnabled ||
            properties.audit.sink == CoordinatorProperties.AuditSink.REDIS

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
     * Lists groups after refreshing any time-based operational state.
     */
    @CriticalSection(operation = "list-groups")
    fun listGroups(): GroupsResponse {
        val now = Instant.now(clock)
        return withStateConflictRetry("list-groups") {
            GroupsResponse(stateStore.list().map {
                if (refreshOperationalState(it, now)) {
                    stateStore.save(it.key(), it)
                }
                recordGroupState(it)
                it.toResponse()
            })
        }
    }

    /**
     * Lists members in a group after applying expiration and rebalance timeout checks.
     */
    @CriticalSection(operation = "list-members")
    fun listMembers(streamPrefix: String, consumerGroup: String): MembersResponse =
        withStateConflictRetry("list-members") {
            val group = requireGroup(streamPrefix, consumerGroup)
            if (refreshOperationalState(group, Instant.now(clock))) {
                stateStore.save(group.key(), group)
            }
            recordGroupState(group)
            MembersResponse(group.members.values.sortedBy { it.memberId })
        }

    /**
     * Returns target/current assignment snapshots plus invariant violations for debugging.
     */
    @CriticalSection(operation = "assignments")
    fun assignments(streamPrefix: String, consumerGroup: String): AssignmentsResponse =
        withStateConflictRetry("assignments") {
            val group = requireGroup(streamPrefix, consumerGroup)
            if (refreshOperationalState(group, Instant.now(clock))) {
                stateStore.save(group.key(), group)
            }
            val violations = invariantViolations(group)
            metrics.recordGroupState(group, violations.size)
            AssignmentsResponse(
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
    @CriticalSection(operation = "consumption-progress")
    fun consumptionProgress(streamPrefix: String, consumerGroup: String): ConsumptionProgressResponse =
        withStateConflictRetry("consumption-progress") {
            val group = requireGroup(streamPrefix, consumerGroup)
            if (refreshOperationalState(group, Instant.now(clock))) {
                stateStore.save(group.key(), group)
            }
            recordGroupState(group)
            ConsumptionProgressResponse(group.toConsumerShardProgress())
        }

    /**
     * Lists all resharding records for a group.
     */
    @CriticalSection(operation = "migrations")
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
            assignedMaxConcurrency = group.assignedMaxConcurrency(memberName),
            runtimeMaxConcurrency = request.runtimeConsumerCapacity.runtimeMaxConcurrency,
            activeConsumerWorkers = 0,
            currentAssignment = emptySet(),
            revoking = emptySet(),
            lastHeartbeatAt = now,
            memberLeaseExpiresAt = now.plus(properties.memberLeaseTtl),
            rebalanceTimeoutMs = request.rebalanceTimeoutMs ?: properties.memberLeaseTtl.toMillis(),
        ).also { group.members[memberId] = it }

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
        member.assignedMaxConcurrency = group.assignedMaxConcurrency(member.memberName)
        member.rebalanceTimeoutMs = request.rebalanceTimeoutMs ?: member.rebalanceTimeoutMs
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
            val liveMemberIds = group.members.values
                .filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING || it.state == MemberState.LEAVING }
                .map { it.memberId }
                .toSet()
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
        return expired || timedOut || migrationAdvanced
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
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignedMaxConcurrency = existing.assignedMaxConcurrency,
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
        return HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.SYNC_METADATA,
            memberId = memberId,
            memberEpoch = existing.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignedMaxConcurrency = existing.assignedMaxConcurrency,
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
        return HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.REVOKE_PENDING,
            memberId = member.memberId,
            memberEpoch = member.memberEpoch,
            heartbeatIntervalMs = properties.heartbeatInterval.toMillis(),
            groupEpoch = group.groupEpoch,
            assignmentEpoch = group.assignmentEpoch,
            metadataVersion = group.metadataVersion,
            assignedMaxConcurrency = member.assignedMaxConcurrency,
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
            groupEpoch = 0,
            assignmentEpoch = 0,
            metadataVersion = 0,
            assignedMaxConcurrency = 0,
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
     * Starts draining the old stream version once all live members converged on the target assignment.
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
        group.readableVersions = setOf(migration.toVersion)
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        return true
    }

    /**
     * Completes resharding after no live member still owns or revokes shards from the old version.
     */
    private fun completeMigrationDrainIfReady(
        group: GroupMetadata,
        migration: Migration,
        now: Instant,
    ): Boolean {
        val oldVersionStillOwned = group.liveMembers().any { member ->
            member.currentAssignment.any { it.streamVersion == migration.fromVersion } ||
                member.revoking.any { it.streamVersion == migration.fromVersion }
        }
        if (oldVersionStillOwned) {
            return false
        }

        migration.state = MigrationState.DEPRECATED
        migration.updatedAt = now
        group.activeReshardingId = null
        group.readableVersions = setOf(migration.toVersion)
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
                result.getValue(it.memberId).size.toDouble() / group.memberWeight(it)
            }.thenBy { it.memberId })
            result.getValue(owner.memberId).add(shard)
        }

        balanceExistingAssignments(
            result,
            liveMembers.associate { it.memberId to group.memberWeight(it) },
        )

        return result
    }

    /**
     * Moves shards from overloaded members only when the weighted load spread improves.
     */
    private fun balanceExistingAssignments(
        assignments: MutableMap<String, MutableSet<ShardId>>,
        memberWeights: Map<String, Int>,
    ) {
        if (assignments.size <= 1) return

        while (true) {
            val currentSpread = assignmentSpread(assignments, memberWeights)
            val mostLoaded = assignments.maxWith(weightedLoadComparator(memberWeights).thenByDescending { it.key })
            val leastLoaded = assignments.minWith(weightedLoadComparator(memberWeights).thenBy { it.key })
            val movedShard = mostLoaded.value.sortedDescending().firstOrNull { shard ->
                mostLoaded.value.remove(shard)
                leastLoaded.value.add(shard)
                val improves = assignmentSpread(assignments, memberWeights) < currentSpread
                leastLoaded.value.remove(shard)
                mostLoaded.value.add(shard)
                improves
            } ?: return

            mostLoaded.value.remove(movedShard)
            leastLoaded.value.add(movedShard)
        }
    }

    private fun weightedLoadComparator(memberWeights: Map<String, Int>): Comparator<Map.Entry<String, MutableSet<ShardId>>> =
        compareBy { entry -> entry.value.size.toDouble() / memberWeights.getValue(entry.key) }

    private fun assignmentSpread(
        assignments: Map<String, Set<ShardId>>,
        memberWeights: Map<String, Int>,
    ): Double {
        val loads = assignments.map { (memberId, shards) -> shards.size.toDouble() / memberWeights.getValue(memberId) }
        return (loads.maxOrNull() ?: 0.0) - (loads.minOrNull() ?: 0.0)
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
        readableVersions.flatMap { version ->
            val count = shardCountsByVersion.getValue(version)
            (0 until count).map { ShardId(version, it) }
        }.sorted()

    private fun GroupMetadata.liveMembers(): List<MemberMetadata> =
        members.values.filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }

    private fun GroupMetadata.assignedMaxConcurrency(memberName: String): Int =
        consumerConcurrencyPolicy.memberOverrides[memberName]
            ?: consumerConcurrencyPolicy.defaultMaxConcurrency

    private fun GroupMetadata.memberWeight(member: MemberMetadata): Int =
        assignedMaxConcurrency(member.memberName).coerceAtLeast(1)

    private fun GroupMetadata.toResponse(): GroupResponse =
        GroupResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            state = state,
            groupEpoch = groupEpoch,
            assignmentEpoch = assignmentEpoch,
            metadataVersion = metadataVersion,
            activeWriteVersion = activeWriteVersion,
            readableVersions = readableVersions,
            shardCount = shardCountsByVersion.getValue(activeWriteVersion),
            consumerConcurrencyPolicy = consumerConcurrencyPolicy,
            activeMigration = activeReshardingId?.let { migrations[it] },
            targetAssignmentSummary = targetAssignments.mapValues { it.value.size },
            currentAssignmentSummary = members.mapValues { it.value.currentAssignment.size },
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
                    .thenBy { it.shard.streamVersion }
                    .thenBy { it.shard.shardIndex },
            )

    /**
     * Converts the active stream version into metadata consumed by producer-side routers.
     */
    private fun GroupMetadata.toProducerRoutingResponse(): ProducerRoutingResponse {
        val activeShardKeys = streamShardKeys(activeWriteVersion)
        return ProducerRoutingResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            metadataVersion = metadataVersion,
            activeWriteVersion = activeWriteVersion,
            shardCount = shardCountsByVersion.getValue(activeWriteVersion),
            streamKeyPattern = RedisStreamShardKeys.keyPattern(streamPrefix),
            shards = activeShardKeys.map { shardKey ->
                ProducerRoutingShard(
                    streamVersion = shardKey.streamVersion,
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
