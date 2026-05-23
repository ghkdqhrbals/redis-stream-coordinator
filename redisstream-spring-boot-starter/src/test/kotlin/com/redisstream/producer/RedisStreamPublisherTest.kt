package com.redisstream.producer

import com.redisstream.consumer.AssignmentView
import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.HeartbeatRequest
import com.redisstream.consumer.HeartbeatResponse
import com.redisstream.consumer.HeartbeatStatus
import com.redisstream.consumer.ProducerRoutingResponse
import com.redisstream.consumer.ProducerRoutingShard
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RedisStreamPublisherTest {
    @Test
    fun `publisher routes partition key and writes fields to selected stream`() {
        val writer = RecordingRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = writer,
        )

        val published = publisher.publish("order-1", mapOf("payload" to "created"))

        assertEquals("1-0", published.recordId)
        assertEquals(published.streamKey, writer.writes.single().streamKey)
        assertEquals(mapOf("payload" to "created"), writer.writes.single().fields)
        assertEquals(1, published.route.activeWriteVersion)
    }

    @Test
    fun `publisher rejects empty field maps before writing`() {
        val writer = RecordingRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = writer,
        )

        assertFailsWith<IllegalArgumentException> {
            publisher.publish("order-1", emptyMap())
        }
        assertEquals(emptyList(), writer.writes)
    }

    private fun routing(): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            metadataVersion = 1,
            activeWriteVersion = 1,
            shardCount = 2,
            hashAlgorithm = "murmur3",
            hashSeed = "default",
            streamKeyPattern = "orders:v{streamVersion}:shard:{shardIndex}",
            shards = (0 until 2).map { shardIndex ->
                ProducerRoutingShard(
                    streamVersion = 1,
                    shardIndex = shardIndex,
                    streamKey = "orders:v1:shard:$shardIndex",
                    redisSlot = shardIndex,
                )
            },
        )
}

private class RecordingRedisStreamWriter : RedisStreamWriter {
    val writes = mutableListOf<Write>()

    override fun add(streamKey: String, fields: Map<String, String>): String {
        writes += Write(streamKey, fields)
        return "1-0"
    }

    data class Write(
        val streamKey: String,
        val fields: Map<String, String>,
    )
}

private class SingleRoutingClient(
    private val response: ProducerRoutingResponse,
) : CoordinatorClient {
    override fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse =
        HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.RETRY,
            memberId = memberId,
            memberEpoch = request.memberEpoch,
            heartbeatIntervalMs = 0,
            groupEpoch = 0,
            assignmentEpoch = 0,
            metadataVersion = 0,
            assignedMaxConcurrency = 0,
            assignment = AssignmentView(emptySet(), emptySet(), 0),
        )

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        response
}
