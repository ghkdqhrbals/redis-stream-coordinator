package com.redisstream.consumer

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
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

    private fun properties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties().apply {
            streamPrefix = "orders"
            consumerGroup = "orders-consumer"
            memberId = "member-a"
            memberName = "member-a"
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

private class RecordingRedisStreamMessageHandler : RedisStreamMessageHandler {
    val messages = mutableListOf<ConsumedRedisStreamMessage>()

    override fun handle(message: ConsumedRedisStreamMessage) {
        messages += message
    }
}
