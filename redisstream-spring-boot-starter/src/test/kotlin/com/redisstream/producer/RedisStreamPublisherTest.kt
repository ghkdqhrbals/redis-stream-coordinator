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
    fun `publisher invalidates routing cache after write failure so next publish refreshes metadata`() {
        val client = ScriptedPublisherRoutingClient(
            routing(version = 1, activeWriteVersion = 1),
            routing(version = 2, activeWriteVersion = 2),
        )
        val writer = FailingOnceRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = client,
            ),
            writer = writer,
        )

        assertFailsWith<IllegalStateException> {
            publisher.publish("order-1", mapOf("payload" to "created"))
        }
        val published = publisher.publish("order-1", mapOf("payload" to "created"))

        assertEquals(2, client.calls)
        assertEquals(2, published.route.activeWriteVersion)
    }

    @Test
    fun `publisher can opt into stale routing write retry after refreshing metadata`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerRedisStreamProducerMetrics(registry, "orders", "orders-consumer")
        val client = ScriptedPublisherRoutingClient(
            routing(version = 1, activeWriteVersion = 1),
            routing(version = 2, activeWriteVersion = 2),
        )
        val writer = FailingStreamVersionWriter(failingVersion = 1)
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroup = "orders-consumer",
                client = client,
                metrics = metrics,
            ),
            writer = writer,
            metrics = metrics,
            maxAttempts = 2,
        )

        val published = publisher.publish("order-1", mapOf("payload" to "created"))

        assertEquals(2, client.calls)
        assertEquals(2, published.route.activeWriteVersion)
        assertEquals(listOf("orders:v1", "orders:v2"), writer.attemptedVersions)
        assertEquals(
            1.0,
            registry.get("redis_stream_producer_publish_attempt_total")
                .tag("status", "ERROR")
                .tag("attempt", "1")
                .counter()
                .count(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_producer_publish_attempt_total")
                .tag("status", "SUCCESS")
                .tag("attempt", "2")
                .counter()
                .count(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_producer_routing_cache_invalidated_total")
                .tag("reason", "write_failure")
                .counter()
                .count(),
        )
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

    private fun routing(
        version: Long = 1,
        activeWriteVersion: Int = 1,
    ): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            metadataVersion = version,
            activeWriteVersion = activeWriteVersion,
            shardCount = 2,
            streamKeyPattern = "orders:v{streamVersion}:shard:{shardIndex}",
            shards = (0 until 2).map { shardIndex ->
                ProducerRoutingShard(
                    streamVersion = activeWriteVersion,
                    shardIndex = shardIndex,
                    streamKey = "orders:v$activeWriteVersion:shard:$shardIndex",
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

private class FailingOnceRedisStreamWriter : RedisStreamWriter {
    private var failed = false

    override fun add(streamKey: String, fields: Map<String, String>): String {
        if (!failed) {
            failed = true
            error("write failed")
        }
        return "2-0"
    }
}

private class FailingStreamVersionWriter(
    private val failingVersion: Int,
) : RedisStreamWriter {
    val attemptedVersions = mutableListOf<String>()

    override fun add(streamKey: String, fields: Map<String, String>): String {
        attemptedVersions += streamKey.substringBefore(":shard")
        if (streamKey.startsWith("orders:v$failingVersion:")) {
            error("stale stream key")
        }
        return "2-0"
    }
}

private class ScriptedPublisherRoutingClient(
    private vararg val responses: ProducerRoutingResponse,
) : CoordinatorClient {
    var calls = 0
        private set

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
        responses[calls++]
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
