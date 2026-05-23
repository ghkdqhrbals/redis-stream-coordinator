package com.redisstream.consumer

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CoordinatorManagedConsumerTest {
    @Test
    fun `poll once sends heartbeat and notifies assigned shards`() {
        val client = ScriptedCoordinatorClient(
            HeartbeatResponse(
                responseTo = "response-1",
                status = HeartbeatStatus.OK,
                memberId = "member-a",
                memberEpoch = 1,
                heartbeatIntervalMs = 3_000,
                groupEpoch = 1,
                assignmentEpoch = 1,
                metadataVersion = 2,
                assignedMaxConcurrency = 4,
                assignment = AssignmentView(
                    assignedShards = setOf(CoordinatorShard(1, 0), CoordinatorShard(1, 1)),
                    pendingShards = emptySet(),
                    metadataVersion = 2,
                ),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()

        assertEquals(setOf(CoordinatorShard(1, 0), CoordinatorShard(1, 1)), lifecycle.assigned.single())
        assertEquals(0, client.requests.single().memberEpoch)
    }

    @Test
    fun `removed assignments are reported as revoked on next heartbeat`() {
        val client = ScriptedCoordinatorClient(
            HeartbeatResponse(
                responseTo = "response-1",
                status = HeartbeatStatus.OK,
                memberId = "member-a",
                memberEpoch = 1,
                heartbeatIntervalMs = 3_000,
                groupEpoch = 1,
                assignmentEpoch = 1,
                metadataVersion = 2,
                assignedMaxConcurrency = 4,
                assignment = AssignmentView(setOf(CoordinatorShard(1, 0), CoordinatorShard(1, 1)), emptySet(), 2),
            ),
            HeartbeatResponse(
                responseTo = "response-2",
                status = HeartbeatStatus.OK,
                memberId = "member-a",
                memberEpoch = 2,
                heartbeatIntervalMs = 3_000,
                groupEpoch = 2,
                assignmentEpoch = 2,
                metadataVersion = 3,
                assignedMaxConcurrency = 4,
                assignment = AssignmentView(setOf(CoordinatorShard(1, 1)), emptySet(), 3),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(setOf(CoordinatorShard(1, 0)), lifecycle.revoked.single())
        assertTrue(client.requests[1].ownedShards.contains(CoordinatorShard(1, 0)))
    }

    @Test
    fun `incomplete revoke is retried and later reported as revoked`() {
        val revokedShard = CoordinatorShard(1, 0)
        val client = ScriptedCoordinatorClient(
            HeartbeatResponse(
                responseTo = "response-1",
                status = HeartbeatStatus.OK,
                memberId = "member-a",
                memberEpoch = 1,
                heartbeatIntervalMs = 3_000,
                groupEpoch = 1,
                assignmentEpoch = 1,
                metadataVersion = 2,
                assignedMaxConcurrency = 4,
                assignment = AssignmentView(setOf(revokedShard, CoordinatorShard(1, 1)), emptySet(), 2),
            ),
            HeartbeatResponse(
                responseTo = "response-2",
                status = HeartbeatStatus.OK,
                memberId = "member-a",
                memberEpoch = 2,
                heartbeatIntervalMs = 3_000,
                groupEpoch = 2,
                assignmentEpoch = 2,
                metadataVersion = 3,
                assignedMaxConcurrency = 4,
                assignment = AssignmentView(setOf(CoordinatorShard(1, 1)), emptySet(), 3),
            ),
            HeartbeatResponse(
                responseTo = "response-3",
                status = HeartbeatStatus.OK,
                memberId = "member-a",
                memberEpoch = 2,
                heartbeatIntervalMs = 3_000,
                groupEpoch = 2,
                assignmentEpoch = 2,
                metadataVersion = 3,
                assignedMaxConcurrency = 4,
                assignment = AssignmentView(setOf(CoordinatorShard(1, 1)), emptySet(), 3),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        lifecycle.completedRevokes = emptySet()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()
        lifecycle.completedRevokes = setOf(revokedShard)
        consumer.pollOnce()

        assertEquals(listOf(setOf(revokedShard), setOf(revokedShard)), lifecycle.revoked)
        assertEquals(RevokingShardState.REVOKED, client.requests[2].revokingShards.single().state)
    }

    @Test
    fun `new revokes keep earlier draining revoke reports`() {
        val shardA = CoordinatorShard(1, 0)
        val shardB = CoordinatorShard(1, 1)
        val shardC = CoordinatorShard(1, 2)
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(setOf(shardA, shardB, shardC), emptySet(), 2),
            ),
            heartbeatResponse(
                memberEpoch = 2,
                assignment = AssignmentView(setOf(shardB, shardC), emptySet(), 3),
            ),
            heartbeatResponse(
                memberEpoch = 3,
                assignment = AssignmentView(setOf(shardC), emptySet(), 4),
            ),
            heartbeatResponse(
                memberEpoch = 3,
                assignment = AssignmentView(setOf(shardC), emptySet(), 4),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        lifecycle.completedRevokes = emptySet()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()
        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(
            setOf(shardA, shardB),
            client.requests[3].revokingShards
                .filter { it.state == RevokingShardState.DRAINING }
                .map { it.shard }
                .toSet(),
        )
    }

    @Test
    fun `pending shards are notified without being reported as owned`() {
        val pendingShard = CoordinatorShard(1, 1)
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(
                    assignedShards = setOf(CoordinatorShard(1, 0)),
                    pendingShards = setOf(pendingShard),
                    metadataVersion = 2,
                ),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()

        assertEquals(setOf(pendingShard), lifecycle.pending.single())
        assertEquals(emptySet(), client.requests.single().ownedShards)
    }

    @Test
    fun `retry response keeps owned shards and reports full state again`() {
        val assigned = setOf(CoordinatorShard(1, 0), CoordinatorShard(1, 1))
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(assigned, emptySet(), 2),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.RETRY,
                memberEpoch = 1,
                assignment = AssignmentView(assigned, emptySet(), 2),
            ),
        )
        val consumer = CoordinatorManagedConsumer(properties(), client, RecordingShardLifecycle())

        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(assigned, client.requests[1].ownedShards)
        assertEquals(1, client.requests[1].memberEpoch)
    }

    @Test
    fun `fenced response stops local ownership and next heartbeat rejoins`() {
        val assigned = setOf(CoordinatorShard(1, 0))
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(assigned, emptySet(), 2),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.FENCED_MEMBER_EPOCH,
                memberEpoch = 1,
                assignment = AssignmentView(emptySet(), emptySet(), 2),
            ),
            heartbeatResponse(
                memberEpoch = 2,
                assignment = AssignmentView(assigned, emptySet(), 3),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(1, lifecycle.fencedCount)
        assertEquals(0, client.requests[2].memberEpoch)
        assertEquals(emptySet(), client.requests[2].ownedShards)
    }

    @Test
    fun `stop sends graceful leave heartbeat with revoked shards`() {
        val assigned = setOf(CoordinatorShard(1, 0), CoordinatorShard(1, 1))
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(assigned, emptySet(), 2),
            ),
            heartbeatResponse(
                memberEpoch = -1,
                assignment = AssignmentView(emptySet(), emptySet(), 3),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.stop()

        val leaveRequest = client.requests[1]
        assertEquals(-1, leaveRequest.memberEpoch)
        assertEquals(assigned, leaveRequest.ownedShards)
        assertEquals(
            assigned,
            leaveRequest.revokingShards
                .filter { it.state == RevokingShardState.REVOKED && it.inFlight == 0 }
                .map { it.shard }
                .toSet(),
        )
        assertEquals(assigned, lifecycle.revoked.single())
    }

    @Test
    fun `stop can skip graceful leave heartbeat when disabled`() {
        val assigned = setOf(CoordinatorShard(1, 0))
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(assigned, emptySet(), 2),
            ),
        )
        val properties = properties().apply {
            gracefulLeaveOnStop = false
        }
        val consumer = CoordinatorManagedConsumer(properties, client, RecordingShardLifecycle())

        consumer.pollOnce()
        consumer.stop()

        assertEquals(1, client.requests.size)
    }

    @Test
    fun `unsupported local heartbeat protocol version fails before polling coordinator`() {
        val client = ScriptedCoordinatorClient()
        val properties = properties().apply {
            protocolVersion = CoordinatorConsumerProtocol.MAX_HEARTBEAT_VERSION + 1
        }
        val consumer = CoordinatorManagedConsumer(properties, client, RecordingShardLifecycle())

        assertFailsWith<IllegalArgumentException> {
            consumer.pollOnce()
        }
        assertTrue(client.requests.isEmpty())
    }

    private fun properties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties().apply {
            streamPrefix = "orders"
            consumerGroup = "orders-consumer"
            memberId = "member-a"
            memberName = "member-a"
            runtimeMaxConcurrency = 4
        }

    private fun heartbeatResponse(
        status: HeartbeatStatus = HeartbeatStatus.OK,
        memberEpoch: Long,
        assignment: AssignmentView,
    ): HeartbeatResponse =
        HeartbeatResponse(
            responseTo = "response",
            status = status,
            memberId = "member-a",
            memberEpoch = memberEpoch,
            heartbeatIntervalMs = 3_000,
            groupEpoch = assignment.metadataVersion,
            assignmentEpoch = assignment.metadataVersion,
            metadataVersion = assignment.metadataVersion,
            assignedMaxConcurrency = 4,
            assignment = assignment,
        )
}

private class RecordingShardLifecycle : CoordinatorShardLifecycle {
    val assigned = mutableListOf<Set<CoordinatorShard>>()
    val pending = mutableListOf<Set<CoordinatorShard>>()
    val revoked = mutableListOf<Set<CoordinatorShard>>()
    var fencedCount = 0
    var completedRevokes: Set<CoordinatorShard>? = null

    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        assigned += shards
    }

    override fun onPending(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        pending += shards
    }

    override fun onRevoked(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext): Set<CoordinatorShard> {
        revoked += shards
        return completedRevokes ?: shards
    }

    override fun onFenced(context: CoordinatorConsumerContext) {
        fencedCount += 1
    }
}

private class ScriptedCoordinatorClient(
    private vararg val responses: HeartbeatResponse,
) : CoordinatorClient {
    val requests = mutableListOf<HeartbeatRequest>()
    private var index = 0

    override fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse {
        requests += request
        return responses[index++]
    }

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        error("producer routing is not used in this test")
}
