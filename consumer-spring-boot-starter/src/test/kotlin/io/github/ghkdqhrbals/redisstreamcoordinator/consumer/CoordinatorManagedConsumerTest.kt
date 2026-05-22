package io.github.ghkdqhrbals.redisstreamcoordinator.consumer

import kotlin.test.Test
import kotlin.test.assertEquals
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

    private fun properties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties().apply {
            streamPrefix = "orders"
            consumerGroup = "orders-consumer"
            memberId = "member-a"
            memberName = "member-a"
            runtimeMaxConcurrency = 4
        }
}

private class RecordingShardLifecycle : CoordinatorShardLifecycle {
    val assigned = mutableListOf<Set<CoordinatorShard>>()
    val revoked = mutableListOf<Set<CoordinatorShard>>()
    var completedRevokes: Set<CoordinatorShard>? = null

    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        assigned += shards
    }

    override fun onRevoked(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext): Set<CoordinatorShard> {
        revoked += shards
        return completedRevokes ?: shards
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
