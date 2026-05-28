package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorException
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
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
    @Synchronized
    fun createGroup(streamPrefix: String, consumerGroup: String, request: CreateGroupRequest): GroupResponse =
        stateMutex.withCriticalSection("create-group") {
            createGroupOnce(streamPrefix, consumerGroup, request)
        }

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

    @Synchronized
    fun getGroup(streamPrefix: String, consumerGroup: String): GroupResponse =
        stateMutex.withCriticalSection("get-group") {
            withStateConflictRetry("get-group") {
                val group = requireGroup(streamPrefix, consumerGroup)
                if (refreshOperationalState(group, Instant.now(clock))) {
                    stateStore.save(group.key(), group)
                }
                recordGroupState(group)
                group.toResponse()
            }
        }

    @Synchronized
    fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        stateMutex.withCriticalSection("producer-routing") {
            withStateConflictRetry("producer-routing") {
                val group = requireGroup(streamPrefix, consumerGroup)
                if (refreshOperationalState(group, Instant.now(clock))) {
                    stateStore.save(group.key(), group)
                }
                recordGroupState(group)
                group.toProducerRoutingResponse()
            }
        }

    @Synchronized
    fun scaleGroup(streamPrefix: String, consumerGroup: String, request: ScaleGroupRequest): Migration {
        try {
            val migration = stateMutex.withCriticalSection("scale-group") {
                withStateConflictRetry("scale-group") { scaleGroupOnce(streamPrefix, consumerGroup, request) }
            }
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

    private fun scaleGroupOnce(streamPrefix: String, consumerGroup: String, request: ScaleGroupRequest): Migration {
        val group = requireGroup(streamPrefix, consumerGroup)
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

    @Synchronized
    fun updateConsumerConcurrency(
        streamPrefix: String,
        consumerGroup: String,
        request: UpdateConsumerConcurrencyRequest,
    ): ConsumerConcurrencyResponse {
        try {
            val response = stateMutex.withCriticalSection("update-consumer-concurrency") {
                withStateConflictRetry("update-consumer-concurrency") {
                    updateConsumerConcurrencyOnce(streamPrefix, consumerGroup, request)
                }
            }
            metrics.recordConsumerConcurrencyUpdate(streamPrefix, consumerGroup, "SUCCESS")
            return response
        } catch (error: RuntimeException) {
            metrics.recordConsumerConcurrencyUpdate(streamPrefix, consumerGroup, "ERROR")
            throw error
        }
    }

    private fun updateConsumerConcurrencyOnce(
        streamPrefix: String,
        consumerGroup: String,
        request: UpdateConsumerConcurrencyRequest,
    ): ConsumerConcurrencyResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
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

    @Synchronized
    fun getMigration(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration =
        stateMutex.withCriticalSection("get-migration") {
            requireGroup(streamPrefix, consumerGroup).migrations[reshardingId]
                ?: throw CoordinatorException(CoordinatorError.MIGRATION_NOT_FOUND)
        }

    @Synchronized
    fun rollbackMigration(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration =
        stateMutex.withCriticalSection("rollback-migration") {
            withStateConflictRetry("rollback-migration") { rollbackMigrationOnce(streamPrefix, consumerGroup, reshardingId) }
        }

    private fun rollbackMigrationOnce(streamPrefix: String, consumerGroup: String, reshardingId: String): Migration {
        val group = requireGroup(streamPrefix, consumerGroup)
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

    @Synchronized
    fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse {
        val response = stateMutex.withCriticalSection("heartbeat") {
            withStateConflictRetry("heartbeat") { heartbeatOnce(streamPrefix, consumerGroup, memberId, request) }
        }
        metrics.recordHeartbeat(streamPrefix, consumerGroup, response.status)
        return response
    }

    private fun heartbeatOnce(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse {
        if (!properties.protocol.supportsHeartbeat(request.protocolVersion)) {
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

        val ownershipReport = validateOwnershipReport(group, member, request)
            ?: return fenceInvalidOwnershipReport(group, member, request, now)

        member.memberName = request.memberName ?: member.memberName
        member.metadataVersion = request.metadataVersion
        member.runtimeMaxConcurrency = request.runtimeConsumerCapacity.runtimeMaxConcurrency
        member.activeConsumerWorkers =
            (request.runtimeConsumerCapacity.runtimeMaxConcurrency - request.runtimeConsumerCapacity.availableConcurrency)
                .coerceAtLeast(0)
        member.assignedMaxConcurrency = group.assignedMaxConcurrency(member.memberName)
        member.rebalanceTimeoutMs = request.rebalanceTimeoutMs ?: member.rebalanceTimeoutMs
        member.currentAssignment = if (member.state == MemberState.LEAVING) emptySet() else ownershipReport.ownedShards
        member.revoking = ownershipReport.revokingShards
            .filterNot { it.state == RevokingShardState.REVOKED && it.inFlight == 0 }
            .map { it.shard }
            .toSet()
        member.lastHeartbeatAt = now
        member.memberLeaseExpiresAt = now.plus(properties.memberLeaseTtl)

        reconcile(group, now)
        enforceRebalanceTimeouts(group, now)
        advanceMigrationDrainState(group, now)
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

    @Synchronized
    fun health(): HealthResponse {
        val redisStatus = if (requiresRedis()) {
            redisConnectionFactory.ifAvailable?.let { factory ->
                runCatching {
                    factory.connection.use { connection -> connection.ping() }
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

    private fun requiresRedis(): Boolean =
        properties.store.type == CoordinatorProperties.StoreType.REDIS ||
            properties.streams.provisioningEnabled ||
            properties.audit.sink == CoordinatorProperties.AuditSink.REDIS

    @Synchronized
    fun tick(): CoordinatorTickResult {
        return stateMutex.tryCriticalSection("tick") {
            tickOnce()
        } ?: CoordinatorTickResult(scannedGroups = 0, changedGroups = 0)
    }

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

    @Synchronized
    fun listGroups(): GroupsResponse {
        val now = Instant.now(clock)
        return stateMutex.withCriticalSection("list-groups") {
            withStateConflictRetry("list-groups") {
                GroupsResponse(stateStore.list().map {
                    if (refreshOperationalState(it, now)) {
                        stateStore.save(it.key(), it)
                    }
                    recordGroupState(it)
                    it.toResponse()
                })
            }
        }
    }

    @Synchronized
    fun listMembers(streamPrefix: String, consumerGroup: String): MembersResponse =
        stateMutex.withCriticalSection("list-members") {
            withStateConflictRetry("list-members") {
                val group = requireGroup(streamPrefix, consumerGroup)
                if (refreshOperationalState(group, Instant.now(clock))) {
                    stateStore.save(group.key(), group)
                }
                recordGroupState(group)
                MembersResponse(group.members.values.sortedBy { it.memberId })
            }
        }

    @Synchronized
    fun assignments(streamPrefix: String, consumerGroup: String): AssignmentsResponse =
        stateMutex.withCriticalSection("assignments") {
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
        }

    @Synchronized
    fun migrations(streamPrefix: String, consumerGroup: String): MigrationsResponse {
        return stateMutex.withCriticalSection("migrations") {
            val group = requireGroup(streamPrefix, consumerGroup)
            MigrationsResponse(
                migrations = group.migrations.values.sortedBy { it.createdAt },
                activeReshardingId = group.activeReshardingId,
            )
        }
    }

    private fun requireGroup(streamPrefix: String, consumerGroup: String): GroupMetadata =
        stateStore.get(GroupKey(streamPrefix, consumerGroup))
            ?: throw CoordinatorException(CoordinatorError.GROUP_NOT_FOUND)

    private fun refreshGroupOperationalState(key: GroupKey, now: Instant): Boolean {
        val group = stateStore.get(key) ?: return false
        if (!refreshOperationalState(group, now)) {
            return false
        }
        stateStore.save(key, group)
        return true
    }

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

    private data class ValidatedOwnershipReport(
        val ownedShards: Set<ShardId>,
        val revokingShards: List<RevokingShardReport>,
    )

    private fun validateOwnershipReport(
        group: GroupMetadata,
        member: MemberMetadata,
        request: HeartbeatRequest,
    ): ValidatedOwnershipReport? {
        if (request.memberEpoch == 0L) {
            return ValidatedOwnershipReport(emptySet(), emptyList())
        }

        val readableShards = group.readableShards().toSet()
        val previouslyAcceptedShards = member.currentAssignment + member.revoking
        val assignableTargetShards = group.targetAssignments[member.memberId].orEmpty() -
            blockedShards(group, member.memberId)
        val allowedOwnedShards = previouslyAcceptedShards +
            assignableTargetShards.filter { it in readableShards }
        val allowedRevokingShards = previouslyAcceptedShards +
            assignableTargetShards.filter { it in readableShards }
        val reportedOwnedShards = if (member.state == MemberState.LEAVING) emptySet() else request.ownedShards
        val unauthorizedOwnedShards = reportedOwnedShards - allowedOwnedShards
        val unauthorizedRevokingShards = request.revokingShards
            .filterNot { it.state == RevokingShardState.REVOKED && it.inFlight == 0 }
            .map { it.shard }
            .filter { it !in allowedRevokingShards }

        if (unauthorizedOwnedShards.isNotEmpty() || unauthorizedRevokingShards.isNotEmpty()) {
            return null
        }

        return ValidatedOwnershipReport(
            ownedShards = reportedOwnedShards.filter { it in allowedOwnedShards }.toSet(),
            revokingShards = request.revokingShards.filter { it.shard in allowedRevokingShards },
        )
    }

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

    private fun refreshOperationalState(group: GroupMetadata, now: Instant): Boolean {
        val expired = expireMembers(group, now) > 0
        val timedOut = enforceRebalanceTimeouts(group, now)
        val migrationAdvanced = advanceMigrationDrainState(group, now)
        return expired || timedOut || migrationAdvanced
    }

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

    private fun advanceMigrationDrainState(group: GroupMetadata, now: Instant): Boolean {
        val migration = group.activeReshardingId?.let { group.migrations[it] } ?: return false
        return when (migration.state) {
            MigrationState.ACTIVE -> startMigrationDrainIfReady(group, migration, now)
            MigrationState.DRAINING -> completeMigrationDrainIfReady(group, migration, now)
            else -> false
        }
    }

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

    private fun hasPendingConvergence(group: GroupMetadata): Boolean =
        group.targetAssignments.any { (memberId, target) ->
            val member = group.members[memberId] ?: return@any true
            val blocked = blockedShards(group, memberId)
            member.currentAssignment != target - blocked || blocked.isNotEmpty()
        }

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
