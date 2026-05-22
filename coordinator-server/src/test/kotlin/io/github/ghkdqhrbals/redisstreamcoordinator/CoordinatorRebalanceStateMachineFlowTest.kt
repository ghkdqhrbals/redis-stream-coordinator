package io.github.ghkdqhrbals.redisstreamcoordinator

import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

@DisplayName("Coordinator rebalance state machine flows")
class CoordinatorRebalanceStateMachineFlowTest {
    @Nested
    @DisplayName("Large flow")
    inner class LargeFlow {
        @Test
        fun `coordinator tick expires silent member and survivors converge through heartbeat responses`() {
            val harness = RebalanceFlowHarness("flow-large-expire", initialShardCount = 6)
            harness.createGroup()
            harness.joinAndConverge("member-a")
            harness.joinAndConverge("member-b")
            harness.joinAndConverge("member-c")
            val beforeExpiration = harness.targets()

            val detected = harness.keepAliveUntilExpired(
                expiredMemberId = "member-b",
                liveMemberIds = listOf("member-a", "member-c"),
            )
            val rebalancedTargets = harness.targets()
            val staleHeartbeat = harness.heartbeat("member-b")
            val firstMemberA = harness.heartbeat("member-a")
            val firstMemberC = harness.heartbeat("member-c")
            val stillReconciling = harness.group()
            harness.heartbeat("member-a")
            harness.heartbeat("member-c")
            val converged = harness.group()
            val assignments = harness.assignments()

            assertAll(
                { assertEquals(GroupState.RECONCILING, detected.state) },
                { assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, staleHeartbeat.status) },
                { assertEquals(MemberState.EXPIRED, harness.memberState("member-b")) },
                { assertEquals(setOf("member-a", "member-c"), rebalancedTargets.keys) },
                { assertTrue(rebalancedTargets.getValue("member-a").containsAll(beforeExpiration.getValue("member-a"))) },
                { assertTrue(rebalancedTargets.getValue("member-c").containsAll(beforeExpiration.getValue("member-c"))) },
                { assertTargetCoverage(rebalancedTargets, (0 until 6).map { ShardId(1, it) }.toSet()) },
                { assertEquals(3, rebalancedTargets.getValue("member-a").size) },
                { assertEquals(3, rebalancedTargets.getValue("member-c").size) },
                { assertEquals(rebalancedTargets.getValue("member-a"), firstMemberA.assignment.assignedShards) },
                { assertEquals(rebalancedTargets.getValue("member-c"), firstMemberC.assignment.assignedShards) },
                { assertTrue(firstMemberA.assignment.pendingShards.isEmpty()) },
                { assertTrue(firstMemberC.assignment.pendingShards.isEmpty()) },
                { assertEquals(GroupState.RECONCILING, stillReconciling.state) },
                { assertEquals(GroupState.STABLE, converged.state) },
                { assertEquals(rebalancedTargets.getValue("member-a"), assignments.currentAssignments.getValue("member-a")) },
                { assertEquals(rebalancedTargets.getValue("member-c"), assignments.currentAssignments.getValue("member-c")) },
                { assertTrue(assignments.invariantViolations.isEmpty()) },
            )
        }
    }

    @Nested
    @DisplayName("Small flow")
    inner class SmallFlow {
        @Test
        fun `new member receives pending shards until previous owner reports released ownership`() {
            val harness = RebalanceFlowHarness("flow-small-join", initialShardCount = 4)
            harness.createGroup()
            harness.joinAndConverge("member-a")

            val firstMemberB = harness.join("member-b")
            val targetAfterJoin = harness.targets()
            val firstMemberA = harness.heartbeat("member-a")
            val stillPendingMemberB = harness.heartbeat("member-b")
            harness.heartbeat("member-a")
            val assignedMemberB = harness.heartbeat("member-b")
            val finalGroup = harness.group()

            assertAll(
                { assertEquals(GroupState.RECONCILING, finalGroup.state) },
                { assertEquals(targetAfterJoin.getValue("member-b"), firstMemberB.assignment.pendingShards) },
                { assertTrue(firstMemberB.assignment.assignedShards.isEmpty()) },
                { assertEquals(targetAfterJoin.getValue("member-a"), firstMemberA.assignment.assignedShards) },
                { assertEquals(targetAfterJoin.getValue("member-b"), stillPendingMemberB.assignment.pendingShards) },
                { assertEquals(targetAfterJoin.getValue("member-b"), assignedMemberB.assignment.assignedShards) },
                { assertTrue(assignedMemberB.assignment.pendingShards.isEmpty()) },
            )

            harness.heartbeat("member-b")
            assertEquals(GroupState.STABLE, harness.group().state)
        }

        @Test
        fun `one second coordinator ticks enter rebalancing when a silent member lease expires`() {
            val harness = RebalanceFlowHarness("flow-small-expire", initialShardCount = 4)
            harness.createGroup()
            harness.joinAndConverge("member-a")
            harness.joinAndConverge("member-b")
            val before = harness.group()

            val detected = harness.keepAliveUntilExpired(
                expiredMemberId = "member-b",
                liveMemberIds = listOf("member-a"),
            )
            val memberA = harness.heartbeat("member-a")

            assertTrue(detected.groupEpoch > before.groupEpoch)
            assertEquals(detected.groupEpoch, detected.assignmentEpoch)
            assertEquals(GroupState.RECONCILING, detected.state)
            assertEquals(MemberState.EXPIRED, harness.memberState("member-b"))
            assertEquals((0 until 4).map { ShardId(1, it) }.toSet(), memberA.assignment.assignedShards)
        }
    }

    @Nested
    @DisplayName("Micro flow")
    inner class MicroFlow {
        @Test
        fun `redistribution keeps survivor sticky shards while balancing expired owner shards`() {
            val harness = RebalanceFlowHarness("flow-micro-sticky", initialShardCount = 6)
            harness.createGroup()
            harness.joinAndConverge("member-a")
            harness.joinAndConverge("member-b")
            harness.joinAndConverge("member-c")
            val before = harness.targets()

            harness.keepAliveUntilExpired(
                expiredMemberId = "member-b",
                liveMemberIds = listOf("member-a", "member-c"),
            )
            val after = harness.targets()

            assertAll(
                { assertEquals(setOf("member-a", "member-c"), after.keys) },
                { assertTrue(after.getValue("member-a").containsAll(before.getValue("member-a"))) },
                { assertTrue(after.getValue("member-c").containsAll(before.getValue("member-c"))) },
                { assertEquals(3, after.getValue("member-a").size) },
                { assertEquals(3, after.getValue("member-c").size) },
                { assertTargetCoverage(after, (0 until 6).map { ShardId(1, it) }.toSet()) },
                { assertTrue(harness.assignments().invariantViolations.isEmpty()) },
            )
        }
    }
}

private class RebalanceFlowHarness(
    private val streamPrefix: String,
    private val initialShardCount: Int,
    private val consumerGroup: String = "orders-consumer",
) {
    private val clock = RebalanceFlowClock()
    private val consumers = linkedMapOf<String, SimulatedConsumer>()
    private val service = CoordinatorService(
        properties = CoordinatorProperties(
            heartbeatInterval = Duration.ofSeconds(1),
            memberLeaseTtl = Duration.ofSeconds(3),
            defaults = CoordinatorProperties.Defaults(
                initialShardCount = initialShardCount,
                consumerMaxConcurrency = 4,
            ),
        ),
        stateStore = InMemoryCoordinatorStateStore(),
        redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
        streamProvisioner = NoopStreamShardProvisioner,
        clock = clock,
    )

    fun createGroup() {
        service.createGroup(
            streamPrefix,
            consumerGroup,
            CreateGroupRequest(
                initialShardCount = initialShardCount,
                hashAlgorithm = "murmur3",
                requestedBy = "rebalance-flow-test",
            ),
        )
    }

    fun joinAndConverge(memberId: String) {
        join(memberId)
        converge()
    }

    fun join(memberId: String): HeartbeatResponse {
        consumers[memberId] = SimulatedConsumer(memberId)
        return heartbeat(memberId, joining = true)
    }

    fun heartbeat(memberId: String): HeartbeatResponse =
        heartbeat(memberId, joining = false)

    fun keepAliveUntilExpired(expiredMemberId: String, liveMemberIds: List<String>): GroupResponse {
        require(expiredMemberId in consumers) { "expired member must be known to the harness" }
        repeat(3) {
            clock.advance(Duration.ofSeconds(1))
            liveMemberIds.forEach { heartbeat(it) }
        }
        clock.advance(Duration.ofSeconds(1))
        return service.getGroup(streamPrefix, consumerGroup)
    }

    fun converge(maxRounds: Int = 12) {
        repeat(maxRounds) {
            consumers.keys.forEach { heartbeat(it) }
            if (group().state == GroupState.STABLE && assignments().targetMatchesCurrent(consumers.keys)) {
                return
            }
        }
        error("group did not converge for $streamPrefix")
    }

    fun group(): GroupResponse =
        service.getGroup(streamPrefix, consumerGroup)

    fun assignments(): AssignmentsResponse =
        service.assignments(streamPrefix, consumerGroup)

    fun targets(): Map<String, Set<ShardId>> =
        assignments().targetAssignment.mapValues { it.value.toSortedSet() }.toSortedMap()

    fun memberState(memberId: String): MemberState =
        service.listMembers(streamPrefix, consumerGroup).members.single { it.memberId == memberId }.state

    private fun heartbeat(memberId: String, joining: Boolean): HeartbeatResponse {
        val consumer = consumers.getValue(memberId)
        val request = HeartbeatRequest(
            protocolVersion = 1,
            requestId = "flow-$memberId-${consumer.requestSequence++}",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = if (joining) 0 else consumer.memberEpoch,
            rebalanceTimeoutMs = 60_000,
            metadataVersion = consumer.metadataVersion,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
            ownedShards = if (joining) emptySet() else consumer.ownedShards,
        )
        val response = service.heartbeat(streamPrefix, consumerGroup, memberId, request)
        consumer.apply(response)
        return response
    }
}

private data class SimulatedConsumer(
    val memberId: String,
    var memberEpoch: Long = 0,
    var metadataVersion: Long = 0,
    var ownedShards: Set<ShardId> = emptySet(),
    var requestSequence: Int = 0,
) {
    fun apply(response: HeartbeatResponse) {
        if (response.status != HeartbeatStatus.OK) {
            return
        }
        memberEpoch = response.memberEpoch
        metadataVersion = response.metadataVersion
        ownedShards = response.assignment.assignedShards
    }
}

private fun AssignmentsResponse.targetMatchesCurrent(memberIds: Iterable<String>): Boolean =
    memberIds.all { memberId ->
        targetAssignment[memberId].orEmpty() == currentAssignments[memberId].orEmpty()
    }

private fun assertTargetCoverage(targets: Map<String, Set<ShardId>>, expected: Set<ShardId>) {
    val allTargets = targets.values.flatten()
    assertEquals(expected, allTargets.toSet())
    assertEquals(allTargets.size, allTargets.toSet().size)
}

private class RebalanceFlowClock(
    private var current: Instant = Instant.parse("2026-05-21T00:00:00Z"),
    private val zone: ZoneId = ZoneOffset.UTC,
) : Clock() {
    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock =
        RebalanceFlowClock(current, zone)

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
        current = current.plus(duration)
    }
}
