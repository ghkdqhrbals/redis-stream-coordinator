package com.redisstream.consumer

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RedisStreamConsumerLifecycleTest {
    @Test
    fun `assigned shard is polled and handled messages are acknowledged`() {
        val shard = CoordinatorShard(1, 0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:v1:shard:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val handler = RecordingRedisStreamMessageHandler()
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = handler,
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())
        val polled = lifecycle.pollOnce(shard)

        assertEquals(1, polled)
        assertEquals(listOf("orders:v1:shard:0"), reader.reads.map { it.streamKey })
        assertEquals(listOf("1-0"), reader.acks.map { it.recordId })
        assertEquals(listOf(mapOf("payload" to "created")), handler.messages.map { it.fields })
    }

    @Test
    fun `consumer lifecycle records message success and ack metrics`() {
        val shard = CoordinatorShard(1, 0)
        val registry = SimpleMeterRegistry()
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:v1:shard:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RecordingRedisStreamMessageHandler(),
            startPollersOnAssignment = false,
            metrics = MicrometerCoordinatorConsumerMetrics(registry, "orders", "orders-consumer", "member-a"),
        )

        lifecycle.onAssigned(setOf(shard), context())
        lifecycle.pollOnce(shard)

        assertEquals(1.0, registry.get("redis_stream_consumer_messages_total").tag("status", "SUCCESS").counter().count())
        assertEquals(1.0, registry.get("redis_stream_consumer_ack_total").counter().count())
        assertEquals(
            1.0,
            registry.get("redis_stream_consumer_ack_status_total").tag("status", "SUCCESS").counter().count(),
        )
    }

    @Test
    fun `ack failure records error without message success metric`() {
        val shard = CoordinatorShard(1, 0)
        val registry = SimpleMeterRegistry()
        val reader = FailingAckRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:v1:shard:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RecordingRedisStreamMessageHandler(),
            startPollersOnAssignment = false,
            metrics = MicrometerCoordinatorConsumerMetrics(registry, "orders", "orders-consumer", "member-a"),
        )

        lifecycle.onAssigned(setOf(shard), context())

        assertFailsWith<IllegalStateException> {
            lifecycle.pollOnce(shard)
        }

        assertEquals(
            0.0,
            registry.find("redis_stream_consumer_messages_total").tag("status", "SUCCESS").counter()?.count() ?: 0.0,
        )
        assertEquals(1.0, registry.get("redis_stream_consumer_messages_total").tag("status", "ERROR").counter().count())
        assertEquals(1.0, registry.get("redis_stream_consumer_ack_status_total").tag("status", "ERROR").counter().count())
        assertEquals(0.0, registry.find("redis_stream_consumer_ack_total").counter()?.count() ?: 0.0)
    }

    @Test
    fun `revoked assigned shard stops polling and reports revoke completion`() {
        val shard = CoordinatorShard(1, 0)
        val reader = ScriptedRedisStreamReader()
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RecordingRedisStreamMessageHandler(),
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())
        val revoked = lifecycle.onRevoked(setOf(shard), context())
        val polled = lifecycle.pollOnce(shard)

        assertEquals(setOf(shard), revoked)
        assertEquals(0, polled)
        assertTrue(reader.reads.isEmpty())
    }

    @Test
    fun `handler failure leaves message unacked for Redis Stream retry policy`() {
        val shard = CoordinatorShard(1, 0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:v1:shard:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RedisStreamMessageHandler { error("handler failed") },
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())

        assertFailsWith<IllegalStateException> {
            lifecycle.pollOnce(shard)
        }
        assertTrue(reader.acks.isEmpty())
        assertEquals(setOf(shard), lifecycle.onRevoked(setOf(shard), context()))
    }

    @Test
    fun `runtime capacity reflects messages currently handled by redis poller`() {
        val shard = CoordinatorShard(1, 0)
        lateinit var lifecycle: RedisStreamConsumerLifecycle
        val observedCapacity = mutableListOf<RuntimeConsumerCapacity>()
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:v1:shard:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RedisStreamMessageHandler {
                observedCapacity += lifecycle.runtimeCapacity(context())
            },
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())
        lifecycle.pollOnce(shard)

        assertEquals(
            listOf(RuntimeConsumerCapacity(runtimeMaxConcurrency = 4, availableConcurrency = 3)),
            observedCapacity,
        )
        assertEquals(RuntimeConsumerCapacity(runtimeMaxConcurrency = 4, availableConcurrency = 4), lifecycle.runtimeCapacity(context()))
    }

    private fun properties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties().apply {
            streamPrefix = "orders"
            consumerGroup = "orders-consumer"
            memberId = "member-a"
            memberName = "member-a"
            runtimeMaxConcurrency = 4
            redis.pollBatchSize = 2
            redis.pollTimeout = Duration.ofMillis(10)
        }

    private fun context(): CoordinatorConsumerContext =
        CoordinatorConsumerContext(
            memberId = "member-a",
            memberName = "member-a",
            assignedMaxConcurrency = 4,
            metadataVersion = 1,
            groupEpoch = 1,
            assignmentEpoch = 1,
        )
}

private class ScriptedRedisStreamReader(
    private vararg val messages: ConsumedRedisStreamMessage,
) : RedisStreamReader {
    val reads = mutableListOf<Read>()
    val acks = mutableListOf<Ack>()

    override fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage> {
        reads += Read(streamKey, shard, consumerGroup, consumerName, count, block)
        return messages.toList()
    }

    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
        acks += Ack(streamKey, consumerGroup, recordId)
    }

    data class Read(
        val streamKey: String,
        val shard: CoordinatorShard,
        val consumerGroup: String,
        val consumerName: String,
        val count: Long,
        val block: Duration,
    )

    data class Ack(
        val streamKey: String,
        val consumerGroup: String,
        val recordId: String,
    )
}

private class FailingAckRedisStreamReader(
    private vararg val messages: ConsumedRedisStreamMessage,
) : RedisStreamReader {
    override fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage> =
        messages.toList()

    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
        error("ack failed")
    }
}

private class RecordingRedisStreamMessageHandler : RedisStreamMessageHandler {
    val messages = mutableListOf<ConsumedRedisStreamMessage>()

    override fun handle(message: ConsumedRedisStreamMessage) {
        messages += message
    }
}
