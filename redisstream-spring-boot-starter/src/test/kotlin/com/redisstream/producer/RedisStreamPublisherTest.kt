package com.redisstream.producer

import com.redisstream.consumer.AssignmentView
import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.HeartbeatRequest
import com.redisstream.consumer.HeartbeatResponse
import com.redisstream.consumer.HeartbeatStatus
import com.redisstream.consumer.ProducerRoutingResponse
import com.redisstream.consumer.ProducerRoutingShard
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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

    @Test
    fun `publisher convenience payload method writes payload field`() {
        val writer = RecordingRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = writer,
        )

        publisher.publish("order-1", "created")

        assertEquals(mapOf("payload" to "created"), writer.writes.single().fields)
    }

    @Test
    fun `publisher batch method preserves request order`() {
        val writer = RecordingRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = writer,
        )

        val published = publisher.publishAll(
            listOf(
                RedisStreamPublishRequest("order-1", mapOf("payload" to "created")),
                RedisStreamPublishRequest("order-2", mapOf("payload" to "paid")),
            ),
        )

        assertEquals(listOf("created", "paid"), writer.writes.map { it.fields.getValue("payload") })
        assertEquals(2, published.size)
    }

    @Test
    fun `publisher records success and failure metrics`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerRedisStreamProducerMetrics(registry, "orders", "orders-consumer")
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = RecordingRedisStreamWriter(),
            metrics = metrics,
        )
        val failingPublisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = FailingRedisStreamWriter(),
            metrics = metrics,
        )

        publisher.publish("order-1", mapOf("payload" to "created"))
        assertFailsWith<IllegalStateException> {
            failingPublisher.publish("order-2", mapOf("payload" to "failed"))
        }

        assertEquals(1.0, registry.get("redis_stream_producer_publish_total").tag("status", "SUCCESS").counter().count())
        assertEquals(1.0, registry.get("redis_stream_producer_publish_total").tag("status", "ERROR").counter().count())
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

private class FailingRedisStreamWriter : RedisStreamWriter {
    override fun add(streamKey: String, fields: Map<String, String>): String =
        error("write failed")
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
