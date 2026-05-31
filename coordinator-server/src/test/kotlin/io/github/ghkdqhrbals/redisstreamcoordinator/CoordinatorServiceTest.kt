package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.protocol.CoordinatorProtocol
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.mockito.Mockito
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
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

        assertEquals(CoordinatorError.GROUP_ALREADY_EXISTS, error.error)
        assertEquals(CoordinatorError.GROUP_ALREADY_EXISTS.code, error.errorCode)
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
    fun `coordination version is accepted only inside module support range`() {
        val service = service(clock = clock)
        service.createGroup("protocol", "orders-consumer", createGroupRequest())

        val accepted = service.heartbeat(
            "protocol",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = 0,
                protocolVersion = CoordinatorProtocol.CURRENT_COORDINATION_VERSION,
            ),
        )
        val rejectedBelowMinimum = service.heartbeat(
            "protocol",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = 0,
                protocolVersion = CoordinatorProtocol.MIN_COORDINATION_VERSION - 1,
            ),
        )
        val rejectedAboveMaximum = service.heartbeat(
            "protocol",
            "orders-consumer",
            "member-c",
            heartbeat(
                "member-c",
                memberEpoch = 0,
                protocolVersion = CoordinatorProtocol.MAX_COORDINATION_VERSION + 1,
            ),
        )

        assertEquals(HeartbeatStatus.OK, accepted.status)
        assertEquals(HeartbeatStatus.UNSUPPORTED_PROTOCOL, rejectedBelowMinimum.status)
        assertEquals(HeartbeatStatus.UNSUPPORTED_PROTOCOL, rejectedAboveMaximum.status)
    }

    @Test
    fun `health skips redis ping when redis features are disabled`() {
        val redisConnectionFactory = Mockito.mock(RedisConnectionFactory::class.java)
        val service = CoordinatorService(
            properties = CoordinatorProperties(),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = redisProvider(redisConnectionFactory),
            clock = clock,
        )

        val health = service.health()

        assertEquals("UP", health.status)
        assertEquals("NOT_CONFIGURED", health.redis)
        Mockito.verifyNoInteractions(redisConnectionFactory)
    }

    @Test
    fun `health degrades when redis is required and unavailable`() {
        val redisConnectionFactory = Mockito.mock(RedisConnectionFactory::class.java)
        Mockito.`when`(redisConnectionFactory.connection).thenThrow(IllegalStateException("redis down"))
        val service = CoordinatorService(
            properties = CoordinatorProperties(store = CoordinatorProperties.Store(type = CoordinatorProperties.StoreType.REDIS)),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = redisProvider(redisConnectionFactory),
            clock = clock,
        )

        val health = service.health()

        assertEquals("DEGRADED", health.status)
        assertEquals("DOWN", health.redis)
        Mockito.verify(redisConnectionFactory).connection
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
    fun `coordinator tick expires silent members without waiting for another heartbeat`() {
        service.createGroup("tick-orders", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("tick-orders", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "tick-orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        clock.advance(Duration.ofSeconds(16))
        val tick = service.tick()
        val members = service.listMembers("tick-orders", "orders-consumer").members
        val assignments = service.assignments("tick-orders", "orders-consumer")

        assertEquals(1, tick.scannedGroups)
        assertEquals(1, tick.changedGroups)
        assertEquals(MemberState.EXPIRED, members.single().state)
        assertTrue(assignments.targetAssignment.isEmpty())
    }

    @Test
    fun `monitoring read retries operational refresh when state save races with another writer`() {
        val store = CopyingConflictOnceStateStore()
        val service = service(clock, store)
        service.createGroup("monitor-race", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("monitor-race", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "monitor-race",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        clock.advance(Duration.ofSeconds(16))
        store.conflictsBeforeSave = 1
        val group = service.getGroup("monitor-race", "orders-consumer")

        assertEquals(GroupState.EMPTY, group.state)
        assertEquals(1, store.conflictedSaves)
        assertEquals(MemberState.EXPIRED, service.listMembers("monitor-race", "orders-consumer").members.single().state)
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
            migration.reshardingId,
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
            service.rollbackMigration("metrics", "metrics-consumer", "reshard-missing")
        }.exceptionOrNull() as CoordinatorException

        assertEquals(CoordinatorError.MIGRATION_NOT_FOUND, error.error)
        assertEquals(CoordinatorError.MIGRATION_NOT_FOUND.code, error.errorCode)
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
    fun `membership rejoin restores gracefully left member`() {
        service.createGroup("logs", "logs-consumer", createGroupRequest(initialShardCount = 2))
        val joined = service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "logs",
            "logs-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = joined.memberEpoch, ownedShards = joined.assignment.assignedShards),
        )
        service.heartbeat(
            "logs",
            "logs-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = -1,
                ownedShards = joined.assignment.assignedShards,
                revokingShards = joined.assignment.assignedShards.map {
                    RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                },
            ),
        )

        val rejoined = service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        val member = service.listMembers("logs", "logs-consumer").members.single()

        assertEquals(HeartbeatStatus.OK, rejoined.status)
        assertEquals(MemberState.ACTIVE, member.state)
        assertEquals(rejoined.memberEpoch, member.memberEpoch)
        assertEquals(joined.assignment.assignedShards, rejoined.assignment.assignedShards)
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
    fun `heartbeat rejects active member epoch reset`() {
        service.createGroup("epoch-reset", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "epoch-reset",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "epoch-reset",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val resetAttempt = service.heartbeat(
            "epoch-reset",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0, ownedShards = memberA.assignment.assignedShards),
        )
        val members = service.listMembers("epoch-reset", "orders-consumer").members
        val assignments = service.assignments("epoch-reset", "orders-consumer")

        assertEquals(HeartbeatStatus.INVALID_REQUEST, resetAttempt.status)
        assertEquals(memberA.memberEpoch, members.single { it.memberId == "member-a" }.memberEpoch)
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.currentAssignments.getValue("member-a"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `heartbeat rejects client advanced member epoch`() {
        service.createGroup("future-epoch", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "future-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "future-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val future = service.heartbeat(
            "future-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = acknowledged.memberEpoch + 1, ownedShards = memberA.assignment.assignedShards),
        )
        val member = service.listMembers("future-epoch", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.INVALID_REQUEST, future.status)
        assertEquals(acknowledged.memberEpoch, member.memberEpoch)
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), member.currentAssignment)
    }

    @Test
    fun `heartbeat with higher metadata version requests sync to current redis metadata`() {
        service.createGroup("metadata-sync", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val sync = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = acknowledged.memberEpoch + 10,
                metadataVersion = acknowledged.metadataVersion + 10,
                ownedShards = memberA.assignment.assignedShards,
            ),
        )
        val member = service.listMembers("metadata-sync", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.SYNC_METADATA, sync.status)
        assertEquals(acknowledged.metadataVersion, sync.metadataVersion)
        assertEquals(acknowledged.memberEpoch, sync.memberEpoch)
        assertEquals(memberA.assignment.assignedShards, sync.assignment.assignedShards)
        assertEquals(acknowledged.metadataVersion + 10, member.metadataVersion)
        assertEquals(memberA.assignment.assignedShards, member.currentAssignment)

        val retriedAfterLostResponse = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = acknowledged.memberEpoch + 10,
                metadataVersion = acknowledged.metadataVersion + 10,
                ownedShards = memberA.assignment.assignedShards,
            ),
        )

        assertEquals(HeartbeatStatus.SYNC_METADATA, retriedAfterLostResponse.status)
        assertEquals(acknowledged.metadataVersion, retriedAfterLostResponse.metadataVersion)
    }

    @Test
    fun `metadata sync ignores stale revoke reports from discarded higher version`() {
        val group = convergeTwoMemberGroup("metadata-sync-revoke")
        val current = service.getGroup("metadata-sync-revoke", "orders-consumer")
        val staleForeignShard = group.memberATarget.first()
        val staleOwnedByB = group.memberBTarget + staleForeignShard

        val sync = service.heartbeat(
            "metadata-sync-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = group.memberB.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = staleOwnedByB,
            ),
        )
        val correctedB = service.heartbeat(
            "metadata-sync-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = sync.memberEpoch,
                metadataVersion = sync.metadataVersion,
                ownedShards = group.memberBTarget,
                revokingShards = listOf(RevokingShardReport(staleForeignShard, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val correctedA = service.heartbeat(
            "metadata-sync-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = group.memberA.memberEpoch,
                metadataVersion = sync.metadataVersion,
                ownedShards = group.memberATarget,
            ),
        )
        val memberB = service.listMembers("metadata-sync-revoke", "orders-consumer").members.single { it.memberId == "member-b" }

        assertEquals(HeartbeatStatus.SYNC_METADATA, sync.status)
        assertEquals(HeartbeatStatus.REVOKE_PENDING, correctedB.status)
        assertEquals(HeartbeatStatus.OK, correctedA.status)
        assertEquals(group.memberBTarget, memberB.currentAssignment)
        assertTrue(memberB.revoking.isEmpty())
    }

    @Test
    fun `metadata sync serializes concurrent stale revoke reports from multiple members`() {
        val group = convergeTwoMemberGroup("metadata-sync-concurrent-revoke")
        val current = service.getGroup("metadata-sync-concurrent-revoke", "orders-consumer")
        val staleForeignShardForA = group.memberBTarget.first()
        val staleForeignShardForB = group.memberATarget.first()

        val syncA = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = group.memberA.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = group.memberATarget + staleForeignShardForA,
            ),
        )
        val syncB = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = group.memberB.memberEpoch + 6,
                metadataVersion = current.metadataVersion + 6,
                ownedShards = group.memberBTarget + staleForeignShardForB,
            ),
        )

        val correctedA = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = syncA.memberEpoch,
                metadataVersion = syncA.metadataVersion,
                ownedShards = group.memberATarget,
                revokingShards = listOf(RevokingShardReport(staleForeignShardForA, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val correctedB = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = syncB.memberEpoch,
                metadataVersion = syncB.metadataVersion,
                ownedShards = group.memberBTarget,
                revokingShards = listOf(RevokingShardReport(staleForeignShardForB, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val members = service.listMembers("metadata-sync-concurrent-revoke", "orders-consumer").members.associateBy { it.memberId }
        val finalGroup = service.getGroup("metadata-sync-concurrent-revoke", "orders-consumer")

        assertEquals(HeartbeatStatus.SYNC_METADATA, syncA.status)
        assertEquals(HeartbeatStatus.SYNC_METADATA, syncB.status)
        assertEquals(HeartbeatStatus.REVOKE_PENDING, correctedA.status)
        assertEquals(HeartbeatStatus.OK, correctedB.status)
        assertEquals(current.metadataVersion, finalGroup.metadataVersion)
        assertEquals(group.memberATarget, members.getValue("member-a").currentAssignment)
        assertEquals(group.memberBTarget, members.getValue("member-b").currentAssignment)
        assertTrue(members.getValue("member-a").revoking.isEmpty())
        assertTrue(members.getValue("member-b").revoking.isEmpty())
    }

    @Test
    fun `heartbeat rejects unsupported negative member epoch`() {
        service.createGroup("negative-epoch", "orders-consumer", createGroupRequest(initialShardCount = 2))

        val rejected = service.heartbeat(
            "negative-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = -2),
        )

        assertEquals(HeartbeatStatus.INVALID_REQUEST, rejected.status)
        assertTrue(service.listMembers("negative-epoch", "orders-consumer").members.isEmpty())
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
    fun `expired member leave heartbeat stays fenced`() {
        service.createGroup("expired-leave", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "expired-leave",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-leave",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        service.tick()

        val leave = service.heartbeat(
            "expired-leave",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = -1, ownedShards = memberA.assignment.assignedShards),
        )
        val member = service.listMembers("expired-leave", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, leave.status)
        assertEquals(MemberState.EXPIRED, member.state)
        assertEquals(memberA.assignment.assignedShards, member.currentAssignment)
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
    fun `heartbeat fences member that reports pending shard ownership before revoke completes`() {
        service.createGroup("premature-ownership", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        val memberB = service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        val pendingShard = memberB.assignment.pendingShards.single()

        val fenced = service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch, ownedShards = setOf(pendingShard)),
        )
        val members = service.listMembers("premature-ownership", "orders-consumer").members
        val assignments = service.assignments("premature-ownership", "orders-consumer")

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, fenced.status)
        assertTrue(fenced.assignment.assignedShards.isEmpty())
        assertEquals(MemberState.FENCED, members.single { it.memberId == "member-b" }.state)
        assertEquals(emptySet(), assignments.currentAssignments.getValue("member-b"))
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.targetAssignment.getValue("member-a"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `heartbeat fences member that reports shard owned by another active member`() {
        val converged = convergeTwoMemberGroup("foreign-ownership")
        val foreignShard = converged.memberATarget.single()

        val fenced = service.heartbeat(
            "foreign-ownership",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = converged.memberB.memberEpoch,
                ownedShards = converged.memberBTarget + foreignShard,
            ),
        )
        val members = service.listMembers("foreign-ownership", "orders-consumer").members
        val assignments = service.assignments("foreign-ownership", "orders-consumer")

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, fenced.status)
        assertEquals(MemberState.FENCED, members.single { it.memberId == "member-b" }.state)
        assertEquals(emptySet(), assignments.currentAssignments.getValue("member-b"))
        assertEquals(emptySet(), assignments.revokeProgress["member-b"].orEmpty())
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `higher metadata version does not bypass fenced member state`() {
        val converged = convergeTwoMemberGroup("metadata-sync-fenced")
        val foreignShard = converged.memberATarget.single()
        service.heartbeat(
            "metadata-sync-fenced",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = converged.memberB.memberEpoch,
                ownedShards = converged.memberBTarget + foreignShard,
            ),
        )

        val retried = service.heartbeat(
            "metadata-sync-fenced",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = converged.memberB.memberEpoch,
                metadataVersion = converged.memberB.metadataVersion + 10,
                ownedShards = converged.memberBTarget + foreignShard,
            ),
        )

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, retried.status)
        assertTrue(retried.assignment.assignedShards.isEmpty())
    }

    @Test
    fun `duplicate terminal revoke report after release does not fence member`() {
        val converged = convergeTwoMemberGroup("duplicate-revoke")
        val releasedByA = converged.memberBTarget.single()

        val response = service.heartbeat(
            "duplicate-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = converged.memberA.memberEpoch,
                ownedShards = converged.memberATarget,
                revokingShards = listOf(RevokingShardReport(releasedByA, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val memberA = service.listMembers("duplicate-revoke", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.OK, response.status)
        assertEquals(MemberState.ACTIVE, memberA.state)
        assertEquals(converged.memberATarget, memberA.currentAssignment)
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

    @Test
    fun `migration completes after old version shards are drained`() {
        service.createGroup("drain", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val joined = service.heartbeat("drain", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        val oldOwned = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = joined.memberEpoch, ownedShards = joined.assignment.assignedShards),
        )
        val migration = service.scaleGroup(
            "drain",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "scale out"),
        )
        val oldAndNew = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = oldOwned.memberEpoch, ownedShards = oldOwned.assignment.assignedShards),
        )

        val drainingResponse = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = oldAndNew.memberEpoch, ownedShards = oldAndNew.assignment.assignedShards),
        )
        val drainingGroup = service.getGroup("drain", "orders-consumer")
        val drainingMigration = service.getMigration("drain", "orders-consumer", migration.reshardingId)

        assertEquals(MigrationState.DRAINING, drainingMigration.state)
        assertEquals(setOf(2), drainingGroup.readableVersions)
        assertEquals((0 until 3).map { ShardId(2, it) }.toSet(), drainingResponse.assignment.assignedShards)
        assertEquals((0 until 3).map { ShardId(2, it) }.toSet(), service.assignments("drain", "orders-consumer").targetAssignment.getValue("member-a"))

        service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = drainingResponse.memberEpoch,
                ownedShards = drainingResponse.assignment.assignedShards,
                revokingShards = listOf(
                    RevokingShardReport(ShardId(1, 0), RevokingShardState.DRAINING, inFlight = 1),
                    RevokingShardReport(ShardId(1, 1), RevokingShardState.REVOKED, inFlight = 0),
                ),
            ),
        )
        assertEquals(MigrationState.DRAINING, service.getMigration("drain", "orders-consumer", migration.reshardingId).state)

        val completed = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = drainingResponse.memberEpoch,
                ownedShards = drainingResponse.assignment.assignedShards,
                revokingShards = listOf(
                    RevokingShardReport(ShardId(1, 0), RevokingShardState.REVOKED, inFlight = 0),
                    RevokingShardReport(ShardId(1, 1), RevokingShardState.REVOKED, inFlight = 0),
                ),
            ),
        )
        val completedGroup = service.getGroup("drain", "orders-consumer")
        val completedMigration = service.getMigration("drain", "orders-consumer", migration.reshardingId)

        assertEquals(MigrationState.DEPRECATED, completedMigration.state)
        assertEquals(null, completedGroup.activeMigration)
        assertEquals(setOf(2), completedGroup.readableVersions)
        assertEquals((0 until 3).map { ShardId(2, it) }.toSet(), completed.assignment.assignedShards)
    }

    @Test
    fun `migration scale lets live consumers revoke old assignments without fencing`() {
        service.createGroup("live-scale", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = SimulatedConsumer("member-a")
        val memberB = SimulatedConsumer("member-b")

        fun poll(member: SimulatedConsumer): HeartbeatResponse {
            val response = service.heartbeat(
                "live-scale",
                "orders-consumer",
                member.memberId,
                heartbeat(
                    member.memberId,
                    memberEpoch = member.memberEpoch,
                    ownedShards = member.ownedShards,
                    revokingShards = member.revokingShards,
                ),
            )
            member.apply(response)
            return response
        }

        poll(memberA)
        poll(memberA)
        poll(memberB)
        poll(memberA)
        poll(memberB)
        poll(memberB)

        service.scaleGroup(
            "live-scale",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "live scale"),
        )

        repeat(8) {
            assertNotEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, poll(memberA).status)
            assertNotEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, poll(memberB).status)
        }
        val assignments = service.assignments("live-scale", "orders-consumer")
        val members = service.listMembers("live-scale", "orders-consumer").members

        assertTrue(assignments.invariantViolations.isEmpty())
        assertEquals(assignments.targetAssignment, assignments.currentAssignments.filterValues { it.isNotEmpty() })
        assertTrue(members.all { it.state == MemberState.ACTIVE })
        assertTrue(assignments.targetAssignment.values.flatten().all { it.streamVersion == 2 })
    }

    @Test
    fun `producer routing returns active write metadata`() {
        service.createGroup(
            "route-orders",
            "orders-consumer",
            CreateGroupRequest(
                initialShardCount = 2,
                requestedBy = "test",
            ),
        )
        val beforeScale = service.producerRouting("route-orders", "orders-consumer")

        service.scaleGroup(
            "route-orders",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "scale producer writes"),
        )
        val afterScale = service.producerRouting("route-orders", "orders-consumer")

        assertEquals(1, beforeScale.activeWriteVersion)
        assertEquals(2, beforeScale.shardCount)
        assertEquals("route-orders:v{streamVersion}:shard:{shardIndex}", beforeScale.streamKeyPattern)
        assertEquals(listOf("route-orders:v1:shard:0", "route-orders:v1:shard:1"), beforeScale.shards.map { it.streamKey })
        assertEquals(2, afterScale.activeWriteVersion)
        assertEquals(3, afterScale.shardCount)
        assertTrue(afterScale.metadataVersion > beforeScale.metadataVersion)
        assertEquals(
            listOf("route-orders:v2:shard:0", "route-orders:v2:shard:1", "route-orders:v2:shard:2"),
            afterScale.shards.map { it.streamKey },
        )
    }

    @Test
    fun `group creation provisions initial stream version`() {
        val provisioner = RecordingStreamShardProvisioner()
        val service = service(clock, InMemoryCoordinatorStateStore(), provisioner)

        service.createGroup("provision-create", "orders-consumer", createGroupRequest(initialShardCount = 2))

        assertEquals(
            listOf(
                ProvisionedVersion("provision-create", "orders-consumer", streamVersion = 1, shardCount = 2),
            ),
            provisioner.provisioned,
        )
    }

    @Test
    fun `scale provisions next stream version after preparing migration state is committed`() {
        val provisioner = RecordingStreamShardProvisioner()
        val service = service(clock, InMemoryCoordinatorStateStore(), provisioner)

        service.createGroup("provision-scale", "orders-consumer", createGroupRequest(initialShardCount = 2))
        service.scaleGroup(
            "provision-scale",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "provision next version"),
        )

        assertEquals(
            listOf(
                ProvisionedVersion("provision-scale", "orders-consumer", streamVersion = 1, shardCount = 2),
                ProvisionedVersion("provision-scale", "orders-consumer", streamVersion = 2, shardCount = 3),
            ),
            provisioner.provisioned,
        )
    }

    private fun service(clock: Clock): CoordinatorService =
        service(clock, InMemoryCoordinatorStateStore())

    private fun service(
        clock: Clock,
        stateStore: CoordinatorStateStore = InMemoryCoordinatorStateStore(),
        streamProvisioner: StreamShardProvisioner = NoopStreamShardProvisioner,
        properties: CoordinatorProperties = CoordinatorProperties(
            heartbeatInterval = Duration.ofSeconds(3),
            memberLeaseTtl = Duration.ofSeconds(15),
            defaults = CoordinatorProperties.Defaults(
                initialShardCount = 4,
                consumerMaxConcurrency = 4,
            ),
        ),
    ): CoordinatorService =
        CoordinatorService(
            properties = properties,
            stateStore = stateStore,
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = streamProvisioner,
            clock = clock,
        )

    private data class ConvergedTwoMemberGroup(
        val memberA: HeartbeatResponse,
        val memberB: HeartbeatResponse,
        val memberATarget: Set<ShardId>,
        val memberBTarget: Set<ShardId>,
    )

    private data class SimulatedConsumer(
        val memberId: String,
        var memberEpoch: Long = 0,
        var ownedShards: Set<ShardId> = emptySet(),
        var revokingShards: List<RevokingShardReport> = emptyList(),
    ) {
        fun apply(response: HeartbeatResponse) {
            when (response.status) {
                HeartbeatStatus.OK -> {
                    memberEpoch = response.memberEpoch
                    val nextOwned = response.assignment.assignedShards
                    val revoked = ownedShards - nextOwned
                    ownedShards = nextOwned
                    revokingShards = revoked.map {
                        RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                    }
                }
                HeartbeatStatus.SYNC_METADATA, HeartbeatStatus.REVOKE_PENDING -> {
                    memberEpoch = response.memberEpoch
                    val nextOwned = ownedShards.intersect(response.assignment.assignedShards)
                    val revoked = ownedShards - nextOwned
                    ownedShards = nextOwned
                    revokingShards = revoked.map {
                        RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                    }
                }
                HeartbeatStatus.FENCED_MEMBER_EPOCH, HeartbeatStatus.UNKNOWN_MEMBER_ID -> {
                    memberEpoch = 0
                    ownedShards = emptySet()
                    revokingShards = emptyList()
                }
                HeartbeatStatus.RETRY -> Unit
                HeartbeatStatus.UNSUPPORTED_PROTOCOL, HeartbeatStatus.INVALID_REQUEST ->
                    error("Unexpected heartbeat status ${response.status}")
            }
        }
    }

    private fun convergeTwoMemberGroup(streamPrefix: String): ConvergedTwoMemberGroup {
        service.createGroup(streamPrefix, "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberAJoined = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val memberAOwnedAll = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberAJoined.memberEpoch,
                ownedShards = memberAJoined.assignment.assignedShards,
            ),
        )
        val memberBJoined = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        val targetAssignments = service.assignments(streamPrefix, "orders-consumer").targetAssignment
        val memberATarget = targetAssignments.getValue("member-a")
        val memberBTarget = targetAssignments.getValue("member-b")
        val releasedByA = memberAOwnedAll.assignment.assignedShards - memberATarget
        val memberAReleased = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberAOwnedAll.memberEpoch,
                ownedShards = memberATarget,
                revokingShards = releasedByA.map {
                    RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                },
            ),
        )
        val memberBAssigned = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberBJoined.memberEpoch),
        )
        val memberBOwnedTarget = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = memberBAssigned.memberEpoch,
                ownedShards = memberBAssigned.assignment.assignedShards,
            ),
        )

        return ConvergedTwoMemberGroup(
            memberA = memberAReleased,
            memberB = memberBOwnedTarget,
            memberATarget = memberATarget,
            memberBTarget = memberBTarget,
        )
    }

    private fun redisProvider(redisConnectionFactory: RedisConnectionFactory) =
        StaticListableBeanFactory()
            .apply { addBean("redisConnectionFactory", redisConnectionFactory) }
            .getBeanProvider(RedisConnectionFactory::class.java)

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
        revokingShards: List<RevokingShardReport> = emptyList(),
        rebalanceTimeoutMs: Long = 60_000,
        protocolVersion: Int = 1,
        metadataVersion: Long = 0,
    ): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = protocolVersion,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            rebalanceTimeoutMs = rebalanceTimeoutMs,
            metadataVersion = metadataVersion,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
            ownedShards = ownedShards,
            revokingShards = revokingShards,
        )
}

private data class ProvisionedVersion(
    val streamPrefix: String,
    val consumerGroup: String,
    val streamVersion: Int,
    val shardCount: Int,
)

private class RecordingStreamShardProvisioner : StreamShardProvisioner {
    val provisioned = mutableListOf<ProvisionedVersion>()

    override fun provision(plan: RedisStreamShardProvisioningPlan) {
        provisioned += ProvisionedVersion(
            streamPrefix = plan.streamPrefix,
            consumerGroup = plan.consumerGroup,
            streamVersion = plan.streamVersion,
            shardCount = plan.shardCount,
        )
    }
}

private class CopyingConflictOnceStateStore : CoordinatorStateStore {
    private val groups = linkedMapOf<GroupKey, GroupMetadata>()
    var conflictsBeforeSave: Int = 0
    var conflictedSaves: Int = 0
        private set

    override fun contains(key: GroupKey): Boolean =
        key in groups

    override fun get(key: GroupKey): GroupMetadata? =
        groups[key]?.deepCopy()

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean {
        if (key in groups) {
            return false
        }
        val stored = group.deepCopy()
        stored.storeRevision = 1
        group.storeRevision = stored.storeRevision
        groups[key] = stored
        return true
    }

    override fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean {
        if (groups[key]?.storeRevision != expectedRevision) {
            return false
        }
        groups.remove(key)
        return true
    }

    override fun save(key: GroupKey, group: GroupMetadata) {
        if (conflictsBeforeSave > 0) {
            conflictsBeforeSave -= 1
            conflictedSaves += 1
            throw CoordinatorStateConflictException("injected monitoring save conflict")
        }
        val stored = group.deepCopy()
        stored.storeRevision = group.storeRevision + 1
        group.storeRevision = stored.storeRevision
        groups[key] = stored
    }

    override fun list(): List<GroupMetadata> =
        groups.values.map { it.deepCopy() }

    private fun GroupMetadata.deepCopy(): GroupMetadata =
        copy(
            readableVersions = readableVersions.toSet(),
            shardCountsByVersion = shardCountsByVersion.toMutableMap(),
            consumerConcurrencyPolicy = consumerConcurrencyPolicy.copy(
                memberOverrides = consumerConcurrencyPolicy.memberOverrides.toMap(),
            ),
            members = members
                .mapValues { (_, member) ->
                    member.copy(
                        currentAssignment = member.currentAssignment.toSet(),
                        revoking = member.revoking.toSet(),
                    )
                }
                .toMutableMap(),
            targetAssignments = targetAssignments
                .mapValues { (_, shards) -> shards.toMutableSet() }
                .toMutableMap(),
            migrations = migrations.mapValues { (_, migration) -> migration.copy() }.toMutableMap(),
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
