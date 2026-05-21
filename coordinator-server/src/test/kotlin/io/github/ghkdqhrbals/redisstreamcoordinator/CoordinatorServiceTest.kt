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
    fun `duplicate group creation is rejected`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest())

        val error = kotlin.runCatching {
            service.createGroup("orders", "orders-consumer", createGroupRequest())
        }.exceptionOrNull() as CoordinatorException

        assertEquals("GROUP_ALREADY_EXISTS", error.errorCode)
    }

    @Test
    fun `heartbeat with mismatched path member id is rejected`() {
        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-b", memberEpoch = 0),
        )

        assertEquals(HeartbeatStatus.INVALID_REQUEST, response.status)
    }

    @Test
    fun `heartbeat for missing group is rejected as unknown member`() {
        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = 1),
        )

        assertEquals(HeartbeatStatus.UNKNOWN_MEMBER_ID, response.status)
    }

    @Test
    fun `unknown member cannot leave by sending negative epoch`() {
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
    fun `expired member is removed from target assignment and new member receives readable shards`() {
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
    fun `consumer concurrency policy update rebalances target assignments by member weight`() {
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
    fun `rollback restores previous stream version and clears active migration`() {
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
    fun `rollback is rejected for unknown migration`() {
        service.createGroup("metrics", "metrics-consumer", createGroupRequest(initialShardCount = 2))

        val error = kotlin.runCatching {
            service.rollbackMigration("metrics", "metrics-consumer", "mig-missing")
        }.exceptionOrNull() as CoordinatorException

        assertEquals("MIGRATION_NOT_FOUND", error.errorCode)
    }

    @Test
    fun `rejoin resets expired member to active and assigns epoch from current group`() {
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
    fun `first heartbeat assigns all readable shards to first member`() {
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
    fun `moved shard remains pending for new member until previous owner revokes it`() {
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
    fun `scale creates next stream version and includes old and new readable versions`() {
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
        CoordinatorService(
            properties = CoordinatorProperties(
            heartbeatInterval = Duration.ofSeconds(3),
            memberLeaseTtl = Duration.ofSeconds(15),
            defaults = CoordinatorProperties.Defaults(
                initialShardCount = 4,
                consumerMaxConcurrency = 4,
            ),
            ),
            stateStore = InMemoryCoordinatorStateStore(),
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
    ): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            rebalanceTimeoutMs = 60_000,
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
