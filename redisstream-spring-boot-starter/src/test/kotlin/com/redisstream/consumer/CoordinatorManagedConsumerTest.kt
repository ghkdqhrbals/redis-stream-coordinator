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
                assignment = AssignmentView(
                    assignedShards = setOf(CoordinatorShard(0), CoordinatorShard(1)),
                    pendingShards = emptySet(),
                    metadataVersion = 2,
                ),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()

        assertEquals(setOf(CoordinatorShard(0), CoordinatorShard(1)), lifecycle.assigned.single())
        assertEquals(0, client.requests.single().memberEpoch)
        assertEquals(CoordinatorConsumerProtocol.CURRENT_COORDINATION_VERSION, client.requests.single().protocolVersion)
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
                assignment = AssignmentView(setOf(CoordinatorShard(0), CoordinatorShard(1)), emptySet(), 2),
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
                assignment = AssignmentView(setOf(CoordinatorShard(1)), emptySet(), 3),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(setOf(CoordinatorShard(0)), lifecycle.revoked.single())
        assertTrue(client.requests[1].ownedShards.contains(CoordinatorShard(0)))
    }

    @Test
    fun `incomplete revoke is retried and later reported as revoked`() {
        val revokedShard = CoordinatorShard(0)
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
                assignment = AssignmentView(setOf(revokedShard, CoordinatorShard(1)), emptySet(), 2),
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
                assignment = AssignmentView(setOf(CoordinatorShard(1)), emptySet(), 3),
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
                assignment = AssignmentView(setOf(CoordinatorShard(1)), emptySet(), 3),
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
        val shardA = CoordinatorShard(0)
        val shardB = CoordinatorShard(1)
        val shardC = CoordinatorShard(2)
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
        val pendingShard = CoordinatorShard(1)
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(
                    assignedShards = setOf(CoordinatorShard(0)),
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
    fun `sync metadata revokes stale reads without starting new shards`() {
        val oldShard = CoordinatorShard(0)
        val newShard = CoordinatorShard(1)
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(setOf(oldShard), emptySet(), 10),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.SYNC_METADATA,
                memberEpoch = 1,
                assignment = AssignmentView(setOf(newShard), emptySet(), 9),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.REVOKE_PENDING,
                memberEpoch = 1,
                assignment = AssignmentView(emptySet(), setOf(newShard), 9),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(listOf(setOf(oldShard)), lifecycle.assigned)
        assertEquals(setOf(oldShard), lifecycle.revoked.single())
        assertEquals(setOf(newShard), lifecycle.pending.first())
        assertEquals(emptySet(), client.requests[2].ownedShards)
        assertEquals(oldShard, client.requests[2].revokingShards.single().shard)
        assertEquals(9, client.requests[2].metadataVersion)
    }

    @Test
    fun `revoke pending keeps new shards pending until ok`() {
        val oldShard = CoordinatorShard(0)
        val newShard = CoordinatorShard(1)
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(setOf(oldShard), emptySet(), 2),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.REVOKE_PENDING,
                memberEpoch = 1,
                assignment = AssignmentView(setOf(newShard), emptySet(), 2),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.OK,
                memberEpoch = 1,
                assignment = AssignmentView(setOf(newShard), emptySet(), 2),
            ),
        )
        val lifecycle = RecordingShardLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(listOf(setOf(oldShard), setOf(newShard)), lifecycle.assigned)
        assertEquals(setOf(oldShard), lifecycle.revoked.single())
        assertEquals(emptySet(), client.requests[2].ownedShards)
    }

    @Test
    fun `retry response keeps owned shards and reports full state again`() {
        val assigned = setOf(CoordinatorShard(0), CoordinatorShard(1))
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
    fun `managed consumer reports runtime capacity from lifecycle provider`() {
        val lifecycle = CapacityReportingShardLifecycle(RuntimeConsumerCapacity(runtimeMaxConcurrency = 4, availableConcurrency = 2))
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(setOf(CoordinatorShard(0)), emptySet(), 2),
            ),
        )
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()

        assertEquals(RuntimeConsumerCapacity(runtimeMaxConcurrency = 4, availableConcurrency = 2), client.requests.single().runtimeConsumerCapacity)
    }

    @Test
    fun `managed consumer sends shard progress only for owned shards`() {
        val owned = CoordinatorShard(0)
        val other = CoordinatorShard(1)
        val lifecycle = ProgressReportingShardLifecycle(
            listOf(
                ShardConsumptionProgress(
                    shard = owned,
                    streamKey = "orders:0",
                    lastDeliveredId = "10-0",
                    lastAckedId = "9-0",
                    pendingCount = 1,
                ),
                ShardConsumptionProgress(
                    shard = other,
                    streamKey = "orders:1",
                    lastDeliveredId = "11-0",
                    lastAckedId = "11-0",
                ),
            ),
        )
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(setOf(owned), emptySet(), 2),
            ),
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(setOf(owned), emptySet(), 2),
            ),
        )
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        consumer.pollOnce()

        assertEquals(listOf(owned), client.requests[1].shardProgress.map { it.shard })
        assertEquals("9-0", client.requests[1].shardProgress.single().lastAckedId)
    }

    @Test
    fun `managed consumer rejects invalid runtime capacity reports`() {
        val lifecycle = CapacityReportingShardLifecycle(RuntimeConsumerCapacity(runtimeMaxConcurrency = 4, availableConcurrency = 5))
        val consumer = CoordinatorManagedConsumer(properties(), ScriptedCoordinatorClient(), lifecycle)

        assertFailsWith<IllegalArgumentException> {
            consumer.pollOnce()
        }
    }

    @Test
    fun `fenced response stops local ownership and next heartbeat rejoins`() {
        val assigned = setOf(CoordinatorShard(0))
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
    fun `unknown member response resets local state and next heartbeat rejoins`() {
        val assigned = setOf(CoordinatorShard(0))
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(assigned, emptySet(), 2),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.UNKNOWN_MEMBER_ID,
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
        assertEquals(emptyList(), client.requests[2].revokingShards)
    }

    @Test
    fun `unknown member reset survives fenced callback failure`() {
        val assigned = setOf(CoordinatorShard(0))
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(assigned, emptySet(), 2),
            ),
            heartbeatResponse(
                status = HeartbeatStatus.UNKNOWN_MEMBER_ID,
                memberEpoch = 1,
                assignment = AssignmentView(emptySet(), emptySet(), 2),
            ),
            heartbeatResponse(
                memberEpoch = 2,
                assignment = AssignmentView(assigned, emptySet(), 3),
            ),
        )
        val lifecycle = FailingFencedLifecycle()
        val consumer = CoordinatorManagedConsumer(properties(), client, lifecycle)

        consumer.pollOnce()
        assertFailsWith<IllegalStateException> {
            consumer.pollOnce()
        }
        consumer.pollOnce()

        assertEquals(0, client.requests[2].memberEpoch)
        assertEquals(emptySet(), client.requests[2].ownedShards)
        assertEquals(emptyList(), client.requests[2].revokingShards)
    }

    @Test
    fun `stop sends graceful leave heartbeat with revoked shards`() {
        val assigned = setOf(CoordinatorShard(0), CoordinatorShard(1))
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
        val assigned = setOf(CoordinatorShard(0))
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
    fun `coordination version is supplied by the consumer module`() {
        val client = ScriptedCoordinatorClient(
            heartbeatResponse(
                memberEpoch = 1,
                assignment = AssignmentView(emptySet(), emptySet(), 1),
            ),
        )
        val consumer = CoordinatorManagedConsumer(properties(), client, RecordingShardLifecycle())

        consumer.pollOnce()

        assertEquals(CoordinatorConsumerProtocol.CURRENT_COORDINATION_VERSION, client.requests.single().protocolVersion)
    }

    @Test
    fun `consumer module declares coordination version release lifecycle`() {
        val support = CoordinatorConsumerProtocol.VERSIONS.single()

        assertEquals(CoordinatorConsumerProtocol.CURRENT_COORDINATION_VERSION, support.version)
        assertEquals(CoordinatorConsumerProtocolStatus.ACTIVE, support.status)
        assertEquals("0.1.0", support.introducedIn.version)
        assertEquals("1.0.0", support.minimumSupportedUntil.version)
        assertEquals(null, support.deprecatedIn)
        assertEquals(null, support.removedIn)
    }

    @Test
    fun `initial routing validation fails when coordinator has no active shards`() {
        val client = ScriptedCoordinatorClient(
            routing = routingResponse(shardCount = 0),
        )
        val consumer = CoordinatorManagedConsumer(properties(), client, RecordingShardLifecycle())

        val error = assertFailsWith<IllegalArgumentException> {
            consumer.validateInitialRouting()
        }
        assertTrue(error.message!!.contains("has no active shards"))
    }

    private fun properties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties().apply {
            streamPrefix = "orders"
            consumerGroupName = "orders-consumer"
            memberId = "member-a"
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
            assignment = assignment,
        )

    private fun routingResponse(
        shardCount: Int = 2,
    ): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            metadataVersion = 1,
                        shardCount = shardCount,
            streamKeyPattern = "orders:{shardIndex}",
            shards = (0 until shardCount).map { shardIndex ->
                ProducerRoutingShard(
                    shardIndex = shardIndex,
                    streamKey = "orders:$shardIndex",
                    redisSlot = shardIndex,
                )
            },
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

private class CapacityReportingShardLifecycle(
    private val capacity: RuntimeConsumerCapacity,
) : CoordinatorShardLifecycle, CoordinatorRuntimeCapacityProvider {
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
    }

    override fun runtimeCapacity(context: CoordinatorConsumerContext): RuntimeConsumerCapacity =
        capacity
}

private class FailingFencedLifecycle : CoordinatorShardLifecycle {
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
    }

    override fun onFenced(context: CoordinatorConsumerContext) {
        error("fenced callback failed")
    }
}

private class ProgressReportingShardLifecycle(
    private val progress: List<ShardConsumptionProgress>,
) : CoordinatorShardLifecycle, CoordinatorShardProgressProvider {
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
    }

    override fun shardProgress(context: CoordinatorConsumerContext): List<ShardConsumptionProgress> =
        progress
}

private class ScriptedCoordinatorClient(
    private vararg val responses: HeartbeatResponse,
    private val routing: ProducerRoutingResponse? = null,
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
        routing ?: error("producer routing is not used in this test")
}
