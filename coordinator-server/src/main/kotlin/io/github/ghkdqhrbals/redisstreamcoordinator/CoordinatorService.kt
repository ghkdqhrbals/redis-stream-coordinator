package io.github.ghkdqhrbals.redisstreamcoordinator

import org.springframework.beans.factory.ObjectProvider
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Instant

@Service
class CoordinatorService(
    private val properties: CoordinatorProperties,
    private val stateStore: CoordinatorStateStore,
    private val redisConnectionFactory: ObjectProvider<RedisConnectionFactory>,
    private val clock: Clock = Clock.systemUTC(),
) {
    @Synchronized
    fun createGroup(streamPrefix: String, consumerGroup: String, request: CreateGroupRequest): GroupResponse {
        val key = GroupKey(streamPrefix, consumerGroup)
        if (stateStore.contains(key)) {
            throw CoordinatorException(HttpStatus.CONFLICT, "GROUP_ALREADY_EXISTS", "Group already exists")
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
            hashAlgorithm = request.hashAlgorithm,
            hashSeed = request.hashSeed,
            consumerConcurrencyPolicy = policy,
            createdAt = now,
            updatedAt = now,
        )
        reconcile(group, now)
        if (!stateStore.putIfAbsent(key, group)) {
            throw CoordinatorException(HttpStatus.CONFLICT, "GROUP_ALREADY_EXISTS", "Group already exists")
        }
        return group.toResponse()
    }

    @Synchronized
    fun getGroup(streamPrefix: String, consumerGroup: String): GroupResponse =
        requireGroup(streamPrefix, consumerGroup).let { group ->
            if (expireMembers(group, Instant.now(clock))) {
                stateStore.save(group.key(), group)
            }
            group.toResponse()
        }

    @Synchronized
    fun scaleGroup(streamPrefix: String, consumerGroup: String, request: ScaleGroupRequest): Migration =
        withStateConflictRetry { scaleGroupOnce(streamPrefix, consumerGroup, request) }

    private fun scaleGroupOnce(streamPrefix: String, consumerGroup: String, request: ScaleGroupRequest): Migration {
        val group = requireGroup(streamPrefix, consumerGroup)
        val now = Instant.now(clock)
        expireMembers(group, now)

        group.activeMigrationId?.let {
            throw CoordinatorException(HttpStatus.CONFLICT, "ACTIVE_MIGRATION_EXISTS", "Group already has an active migration")
        }

        val fromVersion = group.activeWriteVersion
        val fromShardCount = group.shardCountsByVersion.getValue(fromVersion)
        if (fromShardCount == request.targetShardCount) {
            stateStore.save(group.key(), group)
            return Migration(
                migrationId = "noop",
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
            migrationId = newMigrationId(),
            fromVersion = fromVersion,
            toVersion = toVersion,
            fromShardCount = fromShardCount,
            toShardCount = request.targetShardCount,
            state = MigrationState.ACTIVE,
            createdAt = now,
            updatedAt = now,
        )

        group.migrations[migration.migrationId] = migration
        group.activeMigrationId = migration.migrationId
        group.shardCountsByVersion[toVersion] = request.targetShardCount
        group.activeWriteVersion = toVersion
        group.readableVersions = setOf(fromVersion, toVersion)
        request.consumerConcurrencyPolicy?.let { group.consumerConcurrencyPolicy = it }
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        stateStore.save(group.key(), group)
        return migration
    }

    @Synchronized
    fun updateConsumerConcurrency(
        streamPrefix: String,
        consumerGroup: String,
        request: UpdateConsumerConcurrencyRequest,
    ): ConsumerConcurrencyResponse =
        withStateConflictRetry { updateConsumerConcurrencyOnce(streamPrefix, consumerGroup, request) }

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
            bumpMetadata(group, now, bumpGroupEpoch = true)
            group.members.values.forEach { it.assignedMaxConcurrency = group.assignedMaxConcurrency(it.memberName) }
            reconcile(group, now)
            if (group.targetAssignmentSnapshot() != previousTargetAssignments) {
                group.groupEpoch += 1
                group.assignmentEpoch = group.groupEpoch
            }
            stateStore.save(group.key(), group)
        }

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
    fun getMigration(streamPrefix: String, consumerGroup: String, migrationId: String): Migration =
        requireGroup(streamPrefix, consumerGroup).migrations[migrationId]
            ?: throw CoordinatorException(HttpStatus.NOT_FOUND, "MIGRATION_NOT_FOUND", "Migration not found")

    @Synchronized
    fun rollbackMigration(streamPrefix: String, consumerGroup: String, migrationId: String): Migration =
        withStateConflictRetry { rollbackMigrationOnce(streamPrefix, consumerGroup, migrationId) }

    private fun rollbackMigrationOnce(streamPrefix: String, consumerGroup: String, migrationId: String): Migration {
        val group = requireGroup(streamPrefix, consumerGroup)
        val migration = group.migrations[migrationId]
            ?: throw CoordinatorException(HttpStatus.NOT_FOUND, "MIGRATION_NOT_FOUND", "Migration not found")

        if (migration.state == MigrationState.ROLLED_BACK || migration.state == MigrationState.ROLLING_BACK) {
            return migration
        }
        if (group.activeMigrationId != migrationId || migration.state != MigrationState.ACTIVE) {
            throw CoordinatorException(HttpStatus.UNPROCESSABLE_ENTITY, "ROLLBACK_NOT_ALLOWED", "Migration cannot be rolled back")
        }

        val now = Instant.now(clock)
        migration.state = MigrationState.ROLLED_BACK
        migration.updatedAt = now
        group.activeMigrationId = null
        group.activeWriteVersion = migration.fromVersion
        group.readableVersions = setOf(migration.fromVersion)
        group.shardCountsByVersion.remove(migration.toVersion)
        bumpMetadata(group, now, bumpGroupEpoch = true)
        reconcile(group, now)
        stateStore.save(group.key(), group)
        return migration
    }

    @Synchronized
    fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse =
        withStateConflictRetry { heartbeatOnce(streamPrefix, consumerGroup, memberId, request) }

    private fun heartbeatOnce(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse {
        if (request.protocolVersion != 1) {
            return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNSUPPORTED_PROTOCOL)
        }
        if (request.memberId != memberId) {
            return rejectedHeartbeat(request, memberId, HeartbeatStatus.INVALID_REQUEST)
        }

        val group = try {
            requireGroup(streamPrefix, consumerGroup)
        } catch (error: CoordinatorException) {
            if (error.status == HttpStatus.NOT_FOUND) {
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNKNOWN_MEMBER_ID)
            }
            throw error
        }

        val now = Instant.now(clock)
        expireMembers(group, now)
        val existing = group.members[memberId]
        val member = when {
            request.memberEpoch == 0L -> registerOrRejoinMember(group, memberId, request, now)
            request.memberEpoch == -1L && existing == null ->
                return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNKNOWN_MEMBER_ID)
            request.memberEpoch == -1L -> markLeaving(group, memberId, request, now)
            existing == null -> return rejectedHeartbeat(request, memberId, HeartbeatStatus.UNKNOWN_MEMBER_ID)
            existing.state == MemberState.FENCED || request.memberEpoch > existing.memberEpoch ->
                return fencedHeartbeat(group, request, memberId, existing)
            else -> existing
        }

        member.memberName = request.memberName ?: member.memberName
        member.metadataVersion = request.metadataVersion
        member.runtimeMaxConcurrency = request.runtimeConsumerCapacity.runtimeMaxConcurrency
        member.activeConsumerWorkers =
            (request.runtimeConsumerCapacity.runtimeMaxConcurrency - request.runtimeConsumerCapacity.availableConcurrency)
                .coerceAtLeast(0)
        member.assignedMaxConcurrency = group.assignedMaxConcurrency(member.memberName)
        member.currentAssignment = if (member.state == MemberState.LEAVING) emptySet() else request.ownedShards
        member.revoking = request.revokingShards
            .filterNot { it.state == RevokingShardState.REVOKED && it.inFlight == 0 }
            .map { it.shard }
            .toSet()
        member.lastHeartbeatAt = now
        member.memberLeaseExpiresAt = now.plus(properties.memberLeaseTtl)

        reconcile(group, now)
        if (member.state == MemberState.ACTIVE || member.state == MemberState.STARTING) {
            member.memberEpoch = group.assignmentEpoch
        }
        stateStore.save(group.key(), group)

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
        val redisStatus = redisConnectionFactory.ifAvailable?.let { factory ->
            runCatching {
                factory.connection.use { connection -> connection.ping() }
            }.fold(
                onSuccess = { "UP" },
                onFailure = { "DOWN" },
            )
        } ?: "NOT_CONFIGURED"

        return HealthResponse(
            status = if (redisStatus == "DOWN") "DEGRADED" else "UP",
            coordinatorId = properties.id,
            redis = redisStatus,
            loop = "UP",
        )
    }

    @Synchronized
    fun listGroups(): GroupsResponse {
        val now = Instant.now(clock)
        return GroupsResponse(stateStore.list().map {
            if (expireMembers(it, now)) {
                stateStore.save(it.key(), it)
            }
            it.toResponse()
        })
    }

    @Synchronized
    fun listMembers(streamPrefix: String, consumerGroup: String): MembersResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        if (expireMembers(group, Instant.now(clock))) {
            stateStore.save(group.key(), group)
        }
        return MembersResponse(group.members.values.sortedBy { it.memberId })
    }

    @Synchronized
    fun assignments(streamPrefix: String, consumerGroup: String): AssignmentsResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        if (expireMembers(group, Instant.now(clock))) {
            stateStore.save(group.key(), group)
        }
        return AssignmentsResponse(
            targetAssignment = group.targetAssignments.mapValues { it.value.toSortedSet() },
            currentAssignments = group.members.mapValues { it.value.currentAssignment.toSortedSet() },
            revokeProgress = group.members.mapValues { it.value.revoking.toSortedSet() }.filterValues { it.isNotEmpty() },
            invariantViolations = invariantViolations(group),
        )
    }

    @Synchronized
    fun migrations(streamPrefix: String, consumerGroup: String): MigrationsResponse {
        val group = requireGroup(streamPrefix, consumerGroup)
        return MigrationsResponse(
            migrations = group.migrations.values.sortedBy { it.createdAt },
            activeMigration = group.activeMigrationId,
        )
    }

    private fun requireGroup(streamPrefix: String, consumerGroup: String): GroupMetadata =
        stateStore.get(GroupKey(streamPrefix, consumerGroup))
            ?: throw CoordinatorException(HttpStatus.NOT_FOUND, "GROUP_NOT_FOUND", "Group not found")

    private fun <T> withStateConflictRetry(block: () -> T): T {
        var attempts = 0
        while (true) {
            try {
                return block()
            } catch (error: CoordinatorStateConflictException) {
                attempts += 1
                if (attempts >= 3) {
                    throw CoordinatorException(
                        HttpStatus.CONFLICT,
                        "STATE_VERSION_CONFLICT",
                        "Coordinator state changed concurrently; retry the request",
                    )
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
        ).also { group.members[memberId] = it }

        member.state = MemberState.ACTIVE
        member.memberEpoch = group.groupEpoch.coerceAtLeast(1)
        bumpMetadata(group, now, bumpGroupEpoch = true)
        return member
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

    private fun expireMembers(group: GroupMetadata, now: Instant): Boolean {
        var changed = false
        group.members.values.forEach { member ->
            if ((member.state == MemberState.ACTIVE || member.state == MemberState.STARTING) && now.isAfter(member.memberLeaseExpiresAt)) {
                member.state = MemberState.EXPIRED
                member.memberEpoch = member.memberEpoch + 1
                changed = true
            }
        }
        if (changed) {
            bumpMetadata(group, now, bumpGroupEpoch = true)
            reconcile(group, now)
        }
        return changed
    }

    private fun reconcile(group: GroupMetadata, now: Instant) {
        val liveMembers = group.members.values
            .filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }
            .sortedBy { it.memberId }

        if (liveMembers.isEmpty()) {
            group.targetAssignments.clear()
            group.assignmentEpoch = group.groupEpoch
            group.state = GroupState.EMPTY
            group.updatedAt = now
            return
        }

        group.state = GroupState.ASSIGNING
        group.targetAssignments = computeStickyAssignment(group, liveMembers)
        group.assignmentEpoch = group.groupEpoch
        group.state = if (hasPendingConvergence(group)) GroupState.RECONCILING else GroupState.STABLE
        group.updatedAt = now
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

    private fun GroupMetadata.targetAssignmentSnapshot(): Map<String, Set<ShardId>> =
        targetAssignments
            .mapValues { (_, shards) -> shards.toSortedSet() }
            .toSortedMap()

    private fun GroupMetadata.readableShards(): List<ShardId> =
        readableVersions.flatMap { version ->
            val count = shardCountsByVersion.getValue(version)
            (0 until count).map { ShardId(version, it) }
        }.sorted()

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
            activeMigration = activeMigrationId?.let { migrations[it] },
            targetAssignmentSummary = targetAssignments.mapValues { it.value.size },
            currentAssignmentSummary = members.mapValues { it.value.currentAssignment.size },
        )

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
