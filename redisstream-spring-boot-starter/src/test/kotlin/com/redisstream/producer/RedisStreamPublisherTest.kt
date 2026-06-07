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
                consumerGroupName = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = writer,
        )

        val published = publisher.publish("order-1", mapOf("payload" to "created"))

        assertEquals("1-0", published.recordId)
        assertEquals(published.streamKey, writer.writes.single().streamKey)
        assertEquals(mapOf("payload" to "created"), writer.writes.single().fields)
        assertEquals(1, published.route.metadataVersion)
    }

    @Test
    fun `publisher rejects empty field maps before writing`() {
        val writer = RecordingRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroupName = "orders-consumer",
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
                consumerGroupName = "orders-consumer",
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
                consumerGroupName = "orders-consumer",
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
    fun `publisher batch method applies per message xadd options`() {
        val writer = RecordingRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroupName = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = writer,
        )

        publisher.publishAll(
            listOf(
                RedisStreamPublishRequest(
                    partitionKey = "order-1",
                    fields = mapOf("payload" to "created"),
                    options = RedisStreamPublishOptions(maxLen = 1_000, approximateTrimming = false),
                ),
                RedisStreamPublishRequest(
                    partitionKey = "order-2",
                    fields = mapOf("payload" to "paid"),
                    options = RedisStreamPublishOptions(maxLen = 2_000, approximateTrimming = true),
                ),
            ),
        )

        assertEquals(
            listOf(
                RedisStreamPublishOptions(maxLen = 1_000, approximateTrimming = false),
                RedisStreamPublishOptions(maxLen = 2_000, approximateTrimming = true),
            ),
            writer.writes.map { it.options },
        )
    }

    @Test
    fun `publisher passes per message xadd maxlen options to writer`() {
        val writer = RecordingRedisStreamWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroupName = "orders-consumer",
                client = SingleRoutingClient(routing()),
            ),
            writer = writer,
        )

        publisher.publish(
            partitionKey = "order-1",
            fields = mapOf("payload" to "created"),
            options = RedisStreamPublishOptions(maxLen = 1_000, approximateTrimming = false),
        )

        assertEquals(RedisStreamPublishOptions(maxLen = 1_000, approximateTrimming = false), writer.writes.single().options)
    }

    @Test
    fun `publisher retries write failure after refreshing routing metadata by default`() {
        val client = ScriptedPublisherRoutingClient(
            routing(version = 1, streamKeyPrefix = "orders-old"),
            routing(version = 2, streamKeyPrefix = "orders-new"),
        )
        val writer = FailingFirstAttemptWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroupName = "orders-consumer",
                client = client,
            ),
            writer = writer,
        )

        val published = publisher.publish("order-1", mapOf("payload" to "created"))

        assertEquals(2, client.calls)
        assertEquals(2, published.route.metadataVersion)
        assertEquals(listOf("orders-old", "orders-new"), writer.attemptedStreams.map { it.substringBeforeLast(":") })
    }

    @Test
    fun `publisher surfaces write failure after configured attempts are exhausted`() {
        val client = ScriptedPublisherRoutingClient(
            routing(version = 1),
            routing(version = 2),
        )
        val writer = FailingFirstAttemptWriter()
        val publisher = RoutingRedisStreamPublisher(
            routingCache = ProducerRoutingCache(
                streamPrefix = "orders",
                consumerGroupName = "orders-consumer",
                client = client,
            ),
            writer = writer,
            maxAttempts = 1,
        )

        assertFailsWith<IllegalStateException> {
            publisher.publish("order-1", mapOf("payload" to "created"))
        }

        assertEquals(1, client.calls)
        assertEquals(listOf("orders"), writer.attemptedStreams.map { it.substringBeforeLast(":") })
    }

    private fun routing(
        version: Long = 1,
        streamKeyPrefix: String = "orders",
    ): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            metadataVersion = version,
            shardCount = 2,
            streamKeyPattern = "orders:{shardIndex}",
            shards = (0 until 2).map { shardIndex ->
                ProducerRoutingShard(
                    shardIndex = shardIndex,
                    streamKey = "$streamKeyPrefix:$shardIndex",
                    redisSlot = shardIndex,
                )
            },
        )
}

private class RecordingRedisStreamWriter : RedisStreamWriter {
    val writes = mutableListOf<Write>()

    override fun add(streamKey: String, fields: Map<String, String>): String {
        writes += Write(streamKey, fields, RedisStreamPublishOptions())
        return "1-0"
    }

    override fun add(streamKey: String, fields: Map<String, String>, options: RedisStreamPublishOptions): String {
        writes += Write(streamKey, fields, options)
        return "1-0"
    }

    data class Write(
        val streamKey: String,
        val fields: Map<String, String>,
        val options: RedisStreamPublishOptions,
    )
}

private class FailingFirstAttemptWriter : RedisStreamWriter {
    val attemptedStreams = mutableListOf<String>()
    private var failed = false

    override fun add(streamKey: String, fields: Map<String, String>): String {
        attemptedStreams += streamKey
        if (!failed) {
            failed = true
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
            assignment = AssignmentView(emptySet(), emptySet(), 0),
        )

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        response
}
