package com.redisstream.consumer

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RedisStreamConsumerLifecycleTest {
    @Test
    fun `assigned shard is polled and handler can explicitly acknowledge message`() {
        val shard = CoordinatorShard(0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val handler = AckingRedisStreamMessageHandler()
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = handler,
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())
        val polled = lifecycle.pollOnce(shard)

        assertEquals(1, polled)
        assertEquals(listOf("orders:0"), reader.reads.map { it.streamKey })
        assertEquals(listOf("1-0"), reader.acks.map { it.recordId })
        assertEquals(listOf(mapOf("payload" to "created")), handler.messages.map { it.fields })
    }

    @Test
    fun `consumer lifecycle reports delivered and explicitly acked stream progress`() {
        val shard = CoordinatorShard(0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
                recordId = "10-1",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RedisStreamMessageHandler { it.ack() },
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())
        lifecycle.pollOnce(shard)

        val progress = lifecycle.shardProgress(context()).single()
        assertEquals(shard, progress.shard)
        assertEquals("orders:0", progress.streamKey)
        assertEquals("10-1", progress.lastDeliveredId)
        assertEquals("10-1", progress.lastAckedId)
        assertEquals(0L, progress.pendingCount)
    }

    @Test
    fun `manual ack failure propagates without advancing ack progress`() {
        val shard = CoordinatorShard(0)
        val reader = FailingAckRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RedisStreamMessageHandler { it.ack() },
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())

        assertFailsWith<IllegalStateException> {
            lifecycle.pollOnce(shard)
        }

        val progress = lifecycle.shardProgress(context()).single()
        assertEquals("1-0", progress.lastDeliveredId)
        assertEquals(null, progress.lastAckedId)
    }

    @Test
    fun `revoked assigned shard stops polling and reports revoke completion`() {
        val shard = CoordinatorShard(0)
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
        val shard = CoordinatorShard(0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
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
    fun `handler can explicitly release message with xnack`() {
        val shard = CoordinatorShard(0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties(),
            reader = reader,
            handler = RedisStreamMessageHandler {
                it.nack()
                error("handler failed")
            },
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())

        assertFailsWith<IllegalStateException> {
            lifecycle.pollOnce(shard)
        }
        assertEquals(listOf("1-0"), reader.nacks.map { it.recordId })
    }

    @Test
    fun `handler can explicitly acknowledge and delete message`() {
        val shard = CoordinatorShard(0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties().apply {
                redis.ack.xackdelReferencePolicy = RedisStreamXAckDelReferencePolicy.DELREF
            },
            reader = reader,
            handler = RedisStreamMessageHandler { it.ackDel() },
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())
        lifecycle.pollOnce(shard)

        assertTrue(reader.acks.isEmpty())
        assertEquals(listOf(RedisStreamXAckDelReferencePolicy.DELREF), reader.ackDels.map { it.referencePolicy })
        assertEquals("1-0", lifecycle.shardProgress(context()).single().lastAckedId)
    }

    @Test
    fun `runtime capacity reflects messages currently handled by redis poller`() {
        val shard = CoordinatorShard(0)
        lateinit var lifecycle: RedisStreamConsumerLifecycle
        val observedCapacity = mutableListOf<RuntimeConsumerCapacity>()
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
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

    @Test
    fun `poll once limits redis read count to available runtime capacity`() {
        val shard = CoordinatorShard(0)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
                recordId = "1-0",
                shard = shard,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties().apply {
                runtimeMaxConcurrency = 1
                redis.pollBatchSize = 10
            },
            reader = reader,
            handler = RecordingRedisStreamMessageHandler(),
            startPollersOnAssignment = false,
        )

        lifecycle.onAssigned(setOf(shard), context())
        lifecycle.pollOnce(shard)

        assertEquals(1, reader.reads.single().count)
    }

    @Test
    fun `automatic pollers rotate through all assigned shards when assignments exceed concurrency`() {
        val shards = (0..5).map(::CoordinatorShard).toSet()
        val reader = EmptyRecordingRedisStreamReader()
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties().apply {
                runtimeMaxConcurrency = 1
                redis.pollBatchSize = 1
                redis.pollTimeout = Duration.ofMillis(5)
            },
            reader = reader,
            handler = RecordingRedisStreamMessageHandler(),
        )

        lifecycle.onAssigned(shards, context())

        assertTrue(waitUntil { reader.readShards().containsAll(shards) })
        lifecycle.close()
    }

    @Test
    fun `single member poller handles records from every assigned shard without starvation`() {
        val shards = (0..5).map(::CoordinatorShard).toSet()
        val handledShards = CopyOnWriteArrayList<CoordinatorShard>()
        val reader = RepeatingRecordingRedisStreamReader()
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties().apply {
                runtimeMaxConcurrency = 1
                redis.pollBatchSize = 1
                redis.pollTimeout = Duration.ofMillis(5)
            },
            reader = reader,
            handler = RedisStreamMessageHandler { message ->
                handledShards += message.shard
                message.ack()
            },
        )

        try {
            lifecycle.onAssigned(shards, context())

            assertTrue(waitUntil { handledShards.toSet().containsAll(shards) })
            assertEquals(shards, handledShards.toSet())
            assertTrue(reader.readShards().containsAll(shards))
        } finally {
            lifecycle.close()
        }
    }

    @Test
    fun `polling another shard skips redis read while runtime capacity is exhausted`() {
        val shardA = CoordinatorShard(0)
        val shardB = CoordinatorShard(1)
        val enteredHandler = CountDownLatch(1)
        val releaseHandler = CountDownLatch(1)
        val reader = ScriptedRedisStreamReader(
            ConsumedRedisStreamMessage(
                streamKey = "orders:0",
                recordId = "1-0",
                shard = shardA,
                fields = mapOf("payload" to "created"),
            ),
        )
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties().apply {
                runtimeMaxConcurrency = 1
                redis.pollBatchSize = 10
            },
            reader = reader,
            handler = RedisStreamMessageHandler {
                enteredHandler.countDown()
                assertTrue(releaseHandler.await(5, TimeUnit.SECONDS))
            },
            startPollersOnAssignment = false,
        )
        lifecycle.onAssigned(setOf(shardA, shardB), context())
        val executor = Executors.newSingleThreadExecutor()

        val firstPoll = executor.submit<Int> { lifecycle.pollOnce(shardA) }
        assertTrue(enteredHandler.await(5, TimeUnit.SECONDS))
        val skipped = lifecycle.pollOnce(shardB)
        releaseHandler.countDown()

        assertEquals(1, firstPoll.get(5, TimeUnit.SECONDS))
        assertEquals(0, skipped)
        assertEquals(listOf(shardA), reader.reads.map { it.shard })
        executor.shutdownNow()
    }

    @Test
    fun `same shard is never polled concurrently by multiple workers`() {
        val shard = CoordinatorShard(0)
        val readStarted = CountDownLatch(1)
        val releaseRead = CountDownLatch(1)
        val reader = BlockingRedisStreamReader(readStarted, releaseRead)
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = properties().apply {
                runtimeMaxConcurrency = 2
                redis.pollBatchSize = 1
            },
            reader = reader,
            handler = RecordingRedisStreamMessageHandler(),
            startPollersOnAssignment = false,
        )
        lifecycle.onAssigned(setOf(shard), context())
        val executor = Executors.newFixedThreadPool(2)

        val firstPoll = executor.submit<Int> { lifecycle.pollOnce(shard) }
        assertTrue(readStarted.await(5, TimeUnit.SECONDS))
        val skipped = executor.submit<Int> { lifecycle.pollOnce(shard) }

        try {
            assertEquals(0, skipped.get(5, TimeUnit.SECONDS))
        } finally {
            releaseRead.countDown()
        }
        assertEquals(0, firstPoll.get(5, TimeUnit.SECONDS))
        assertEquals(1, reader.readCount())
        executor.shutdownNow()
    }

    private fun properties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties().apply {
            streamPrefix = "orders"
            consumerGroupName = "orders-consumer"
            memberId = "member-a"
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

    private fun waitUntil(timeout: Duration = Duration.ofSeconds(5), condition: () -> Boolean): Boolean {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            if (condition()) {
                return true
            }
            Thread.sleep(10)
        }
        return condition()
    }
}

private class ScriptedRedisStreamReader(
    private vararg val messages: ConsumedRedisStreamMessage,
) : RedisStreamReader {
    val reads = mutableListOf<Read>()
    val acks = mutableListOf<Ack>()
    val ackDels = mutableListOf<AckDel>()
    val nacks = mutableListOf<Nack>()

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

    override fun ackDel(
        streamKey: String,
        consumerGroup: String,
        recordId: String,
        referencePolicy: RedisStreamXAckDelReferencePolicy,
    ) {
        ackDels += AckDel(streamKey, consumerGroup, recordId, referencePolicy)
    }

    override fun nack(
        streamKey: String,
        consumerGroup: String,
        recordId: String,
        mode: RedisStreamXNackMode,
        retryCount: Long?,
        force: Boolean,
    ) {
        nacks += Nack(streamKey, consumerGroup, recordId, mode, retryCount, force)
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

    data class AckDel(
        val streamKey: String,
        val consumerGroup: String,
        val recordId: String,
        val referencePolicy: RedisStreamXAckDelReferencePolicy,
    )

    data class Nack(
        val streamKey: String,
        val consumerGroup: String,
        val recordId: String,
        val mode: RedisStreamXNackMode,
        val retryCount: Long?,
        val force: Boolean,
    )
}

private class EmptyRecordingRedisStreamReader : RedisStreamReader {
    private val reads = CopyOnWriteArrayList<ScriptedRedisStreamReader.Read>()

    override fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage> {
        reads += ScriptedRedisStreamReader.Read(streamKey, shard, consumerGroup, consumerName, count, block)
        return emptyList()
    }

    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
    }

    fun readShards(): Set<CoordinatorShard> =
        reads.map { it.shard }.toSet()
}

private class RepeatingRecordingRedisStreamReader : RedisStreamReader {
    private val sequence = AtomicInteger(0)
    private val reads = CopyOnWriteArrayList<ScriptedRedisStreamReader.Read>()
    private val acks = CopyOnWriteArrayList<String>()

    override fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage> {
        reads += ScriptedRedisStreamReader.Read(streamKey, shard, consumerGroup, consumerName, count, block)
        val recordId = "${sequence.incrementAndGet()}-0"
        return listOf(
            ConsumedRedisStreamMessage(
                streamKey = streamKey,
                recordId = recordId,
                shard = shard,
                fields = mapOf("payload" to "created-$recordId"),
            ),
        )
    }

    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
        acks += recordId
    }

    fun readShards(): Set<CoordinatorShard> =
        reads.map { it.shard }.toSet()
}

private class BlockingRedisStreamReader(
    private val readStarted: CountDownLatch,
    private val releaseRead: CountDownLatch,
) : RedisStreamReader {
    private val reads = AtomicInteger(0)

    override fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage> {
        reads.incrementAndGet()
        readStarted.countDown()
        assertTrue(releaseRead.await(5, TimeUnit.SECONDS))
        return emptyList()
    }

    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
    }

    fun readCount(): Int = reads.get()
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

private class AckingRedisStreamMessageHandler : RedisStreamMessageHandler {
    val messages = mutableListOf<ConsumedRedisStreamMessage>()

    override fun handle(message: ConsumedRedisStreamMessage) {
        messages += message
        message.ack()
    }
}
