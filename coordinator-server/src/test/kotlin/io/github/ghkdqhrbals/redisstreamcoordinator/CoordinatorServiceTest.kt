package io.github.ghkdqhrbals.redisstreamcoordinator

import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import java.time.ZoneId
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class CoordinatorServiceTest {
    private val clock = MutableClock(Instant.parse("2026-05-21T00:00:00Z"), ZoneOffset.UTC)
    private val service = service(clock)

    @Test
    fun `group rejects duplicate create`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest())

        val error = kotlin.runCatching {
            service.createGroup("orders", "orders-consumer", createGroupRequest())
        }.exceptionOrNull() as CoordinatorException

        assertEquals("GROUP_ALREADY_EXISTS", error.errorCode)
    }

    @Test
    fun `heartbeat rejects member id mismatch`() {
        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-b", memberEpoch = 0),
        )

        assertEquals(HeartbeatStatus.INVALID_REQUEST, response.status)
    }

    @Test
    fun `heartbeat returns unknown member for missing group`() {
        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = 1),
        )

        assertEquals(HeartbeatStatus.UNKNOWN_MEMBER_ID, response.status)
    }

    @Test
    fun `heartbeat rejects unknown member leave`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest())

        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = -1),
        )

        assertEquals(HeartbeatStatus.UNKNOWN_MEMBER_ID, response.status)
        assertTrue(service.listMembers("orders", "orders-consumer").members.isEmpty())
    }

    @Test
    fun `membership expires owner and reassigns shards`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("orders", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        clock.advance(Duration.ofSeconds(16))
        val second = service.heartbeat("orders", "orders-consumer", "member-b", heartbeat("member-b", memberEpoch = 0))
        val assignments = service.assignments("orders", "orders-consumer")

        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.targetAssignment.getValue("member-b"))
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), second.assignment.assignedShards)
        assertTrue(second.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `capacity rebalances by member weight`() {
        service.createGroup("events", "events-consumer", createGroupRequest(initialShardCount = 8))
        val first = service.heartbeat("events", "events-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "events",
            "events-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        service.heartbeat("events", "events-consumer", "member-b", heartbeat("member-b", memberEpoch = 0))

        service.updateConsumerConcurrency(
            "events",
            "events-consumer",
            UpdateConsumerConcurrencyRequest(
                defaultMaxConcurrency = 1,
                memberOverrides = mapOf("member-b" to 3),
                requestedBy = "test",
                reason = "member-b has more workers",
            ),
        )

        val assignments = service.assignments("events", "events-consumer").targetAssignment

        assertEquals(2, assignments.getValue("member-a").size)
        assertEquals(6, assignments.getValue("member-b").size)
    }

    @Test
    fun `capacity movement advances epochs`() {
        service.createGroup("policy-epochs", "events-consumer", createGroupRequest(initialShardCount = 8))
        val first = service.heartbeat(
            "policy-epochs",
            "events-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "policy-epochs",
            "events-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        val second = service.heartbeat(
            "policy-epochs",
            "events-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        val before = service.getGroup("policy-epochs", "events-consumer")

        val update = service.updateConsumerConcurrency(
            "policy-epochs",
            "events-consumer",
            UpdateConsumerConcurrencyRequest(
                defaultMaxConcurrency = 1,
                memberOverrides = mapOf("member-b" to 3),
                requestedBy = "test",
                reason = "member-b has more workers",
            ),
        )
        val after = service.getGroup("policy-epochs", "events-consumer")
        val memberA = service.heartbeat(
            "policy-epochs",
            "events-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        val memberB = service.heartbeat(
            "policy-epochs",
            "events-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = second.memberEpoch),
        )

        assertTrue(update.groupEpoch > before.groupEpoch)
        assertEquals(before.groupEpoch + 1, update.groupEpoch)
        assertEquals(update.groupEpoch, after.assignmentEpoch)
        assertEquals(after.assignmentEpoch, memberA.memberEpoch)
        assertEquals(after.assignmentEpoch, memberB.memberEpoch)
        assertEquals(2, memberA.assignment.assignedShards.size)
        assertEquals(6, memberB.assignment.pendingShards.size)
        assertTrue(memberB.assignment.assignedShards.isEmpty())
    }

    @Test
    fun `capacity metadata update keeps assignment epoch`() {
        service.createGroup("policy-metadata", "events-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat(
            "policy-metadata",
            "events-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "policy-metadata",
            "events-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        val before = service.getGroup("policy-metadata", "events-consumer")

        val update = service.updateConsumerConcurrency(
            "policy-metadata",
            "events-consumer",
            UpdateConsumerConcurrencyRequest(
                defaultMaxConcurrency = 8,
                requestedBy = "test",
                reason = "single member capacity metadata only",
            ),
        )
        val after = service.getGroup("policy-metadata", "events-consumer")

        assertEquals(before.groupEpoch, update.groupEpoch)
        assertEquals(before.groupEpoch, after.groupEpoch)
        assertEquals(before.assignmentEpoch, after.assignmentEpoch)
        assertTrue(after.metadataVersion > before.metadataVersion)
        assertEquals(mapOf("member-a" to 2), after.targetAssignmentSummary)
    }

    @Test
    fun `migration rollback restores previous version`() {
        service.createGroup("metrics", "metrics-consumer", createGroupRequest(initialShardCount = 2))
        val migration = service.scaleGroup(
            "metrics",
            "metrics-consumer",
            ScaleGroupRequest(targetShardCount = 4, requestedBy = "test", reason = "scale out"),
        )

        val rolledBack = service.rollbackMigration(
            "metrics",
            "metrics-consumer",
            migration.migrationId,
        )
        val group = service.getGroup("metrics", "metrics-consumer")

        assertEquals(MigrationState.ROLLED_BACK, rolledBack.state)
        assertEquals(1, group.activeWriteVersion)
        assertEquals(setOf(1), group.readableVersions)
        assertEquals(null, group.activeMigration)
    }

    @Test
    fun `migration rollback rejects unknown id`() {
        service.createGroup("metrics", "metrics-consumer", createGroupRequest(initialShardCount = 2))

        val error = kotlin.runCatching {
            service.rollbackMigration("metrics", "metrics-consumer", "mig-missing")
        }.exceptionOrNull() as CoordinatorException

        assertEquals("MIGRATION_NOT_FOUND", error.errorCode)
    }

    @Test
    fun `membership rejoin restores expired member`() {
        service.createGroup("logs", "logs-consumer", createGroupRequest(initialShardCount = 2))
        service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        clock.advance(Duration.ofSeconds(16))
        service.getGroup("logs", "logs-consumer")

        val rejoined = service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        val member = service.listMembers("logs", "logs-consumer").members.single()

        assertEquals(HeartbeatStatus.OK, rejoined.status)
        assertEquals(MemberState.ACTIVE, member.state)
        assertEquals(rejoined.memberEpoch, member.memberEpoch)
    }

    @Test
    fun `assignment first member gets readable shards`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest())

        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = 0),
        )

        assertEquals(HeartbeatStatus.OK, response.status)
        assertEquals(
            setOf(ShardId(1, 0), ShardId(1, 1), ShardId(1, 2), ShardId(1, 3)),
            response.assignment.assignedShards,
        )
        assertTrue(response.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `assignment waits for previous owner revoke`() {
        service.createGroup("payments", "payments-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("payments", "payments-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "payments",
            "payments-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        val second = service.heartbeat("payments", "payments-consumer", "member-b", heartbeat("member-b", memberEpoch = 0))

        assertEquals(setOf(ShardId(1, 1)), second.assignment.pendingShards)
        assertTrue(second.assignment.assignedShards.isEmpty())

        val revokeFromA = service.heartbeat(
            "payments",
            "payments-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = first.memberEpoch,
                ownedShards = setOf(ShardId(1, 0)),
                revokingShards = listOf(RevokingShardReport(ShardId(1, 1), RevokingShardState.REVOKED)),
            ),
        )
        assertEquals(setOf(ShardId(1, 0)), revokeFromA.assignment.assignedShards)

        val assignedToB = service.heartbeat(
            "payments",
            "payments-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = second.memberEpoch),
        )
        assertEquals(setOf(ShardId(1, 1)), assignedToB.assignment.assignedShards)
        assertTrue(assignedToB.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `rebalance timeout fences stuck owner`() {
        service.createGroup("rebalance-timeout", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0, rebalanceTimeoutMs = 5_000),
        )
        service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = memberA.assignment.assignedShards,
                rebalanceTimeoutMs = 5_000,
            ),
        )
        val memberB = service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        assertEquals(setOf(ShardId(1, 1)), memberB.assignment.pendingShards)

        clock.advance(Duration.ofSeconds(6))
        val afterTimeout = service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch),
        )
        val members = service.listMembers("rebalance-timeout", "orders-consumer").members
        val assignments = service.assignments("rebalance-timeout", "orders-consumer")

        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), afterTimeout.assignment.assignedShards)
        assertEquals(MemberState.FENCED, members.single { it.memberId == "member-a" }.state)
        assertEquals(MemberState.ACTIVE, members.single { it.memberId == "member-b" }.state)
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `rebalance timeout keeps timely owner active`() {
        service.createGroup("rebalance-completes", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0, rebalanceTimeoutMs = 5_000),
        )
        service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = memberA.assignment.assignedShards,
                rebalanceTimeoutMs = 5_000,
            ),
        )
        val memberB = service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )

        service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = setOf(ShardId(1, 0)),
                revokingShards = listOf(RevokingShardReport(ShardId(1, 1), RevokingShardState.REVOKED)),
                rebalanceTimeoutMs = 5_000,
            ),
        )
        clock.advance(Duration.ofSeconds(6))
        val assignedToB = service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch),
        )
        val members = service.listMembers("rebalance-completes", "orders-consumer").members

        assertEquals(setOf(ShardId(1, 1)), assignedToB.assignment.assignedShards)
        assertEquals(MemberState.ACTIVE, members.single { it.memberId == "member-a" }.state)
    }

    @Test
    fun `failover resumes pending revoke`() {
        val sharedStore = InMemoryCoordinatorStateStore()
        val firstCoordinator = service(clock, sharedStore)
        firstCoordinator.createGroup("failover", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = firstCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        firstCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val memberB = firstCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        assertEquals(setOf(ShardId(1, 1)), memberB.assignment.pendingShards)

        val replacementCoordinator = service(clock, sharedStore)
        val revokeAck = replacementCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = setOf(ShardId(1, 0)),
                revokingShards = listOf(RevokingShardReport(ShardId(1, 1), RevokingShardState.REVOKED)),
            ),
        )
        val assignedToB = replacementCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch),
        )

        assertEquals(setOf(ShardId(1, 0)), revokeAck.assignment.assignedShards)
        assertEquals(setOf(ShardId(1, 1)), assignedToB.assignment.assignedShards)
        assertTrue(replacementCoordinator.assignments("failover", "orders-consumer").invariantViolations.isEmpty())
    }

    @Test
    fun `heartbeat fences stale member epoch`() {
        service.createGroup("stale-epoch", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "stale-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "stale-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val stale = service.heartbeat(
            "stale-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = acknowledged.memberEpoch - 1, ownedShards = memberA.assignment.assignedShards),
        )

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, stale.status)
        assertEquals(acknowledged.memberEpoch, stale.memberEpoch)
        assertTrue(stale.assignment.assignedShards.isEmpty())
        assertTrue(stale.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `membership fences expired owner stale epoch`() {
        service.createGroup("expired-return", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        val memberB = service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )

        val staleMemberA = service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        val assignments = service.assignments("expired-return", "orders-consumer")

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, staleMemberA.status)
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), memberB.assignment.assignedShards)
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.targetAssignment.getValue("member-b"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `membership ignores stale ownership on rejoin`() {
        service.createGroup("expired-rejoin", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        val memberB = service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch, ownedShards = memberB.assignment.assignedShards),
        )

        val rejoinedA = service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0, ownedShards = memberA.assignment.assignedShards),
        )
        val assignments = service.assignments("expired-rejoin", "orders-consumer")

        assertEquals(HeartbeatStatus.OK, rejoinedA.status)
        assertTrue(rejoinedA.assignment.assignedShards.isEmpty())
        assertEquals(emptySet(), assignments.currentAssignments.getValue("member-a"))
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.currentAssignments.getValue("member-b"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `migration scale creates next version`() {
        service.createGroup("summary", "summary-consumer", createGroupRequest(initialShardCount = 2))

        val migration = service.scaleGroup(
            "summary",
            "summary-consumer",
            ScaleGroupRequest(
                targetShardCount = 3,
                requestedBy = "test",
                reason = "scale out",
            ),
        )
        val group = service.getGroup("summary", "summary-consumer")

        assertEquals(1, migration.fromVersion)
        assertEquals(2, migration.toVersion)
        assertEquals(3, group.shardCount)
        assertEquals(setOf(1, 2), group.readableVersions)
    }

    private fun service(clock: Clock): CoordinatorService =
        service(clock, InMemoryCoordinatorStateStore())

    private fun service(clock: Clock, stateStore: CoordinatorStateStore): CoordinatorService =
        CoordinatorService(
            properties = CoordinatorProperties(
                heartbeatInterval = Duration.ofSeconds(3),
                memberLeaseTtl = Duration.ofSeconds(15),
                defaults = CoordinatorProperties.Defaults(
                    initialShardCount = 4,
                    consumerMaxConcurrency = 4,
                ),
            ),
            stateStore = stateStore,
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            clock = clock,
        )

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            hashAlgorithm = "murmur3",
            requestedBy = "test",
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
        revokingShards: List<RevokingShardReport> = emptyList(),
        rebalanceTimeoutMs: Long = 60_000,
    ): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            rebalanceTimeoutMs = rebalanceTimeoutMs,
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
            ownedShards = ownedShards,
            revokingShards = revokingShards,
        )
}

private class MutableClock(
    private var current: Instant,
    private val zone: ZoneId,
) : Clock() {
    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock =
        MutableClock(current, zone)

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
        current = current.plus(duration)
    }
}
