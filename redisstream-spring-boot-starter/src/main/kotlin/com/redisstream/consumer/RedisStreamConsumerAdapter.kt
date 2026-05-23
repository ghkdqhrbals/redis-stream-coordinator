package com.redisstream.consumer

import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.StreamReadOptions
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean

data class ConsumedRedisStreamMessage(
    val streamKey: String,
    val recordId: String,
    val shard: CoordinatorShard,
    val fields: Map<String, String>,
)

fun interface RedisStreamMessageHandler {
    fun handle(message: ConsumedRedisStreamMessage)
}

class RedisStreamConsumerLifecycle(
    private val properties: CoordinatorConsumerProperties,
    private val reader: RedisStreamReader,
    private val handler: RedisStreamMessageHandler,
    private val startPollersOnAssignment: Boolean = true,
) : CoordinatorShardLifecycle, AutoCloseable {
    private val shardStates = ConcurrentHashMap<CoordinatorShard, ShardState>()
    private val executor: ExecutorService = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "redis-stream-consumer-${properties.memberId}").apply {
            isDaemon = true
        }
    }

    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        shards.forEach { shard ->
            val state = shardStates.computeIfAbsent(shard) { ShardState() }
            state.stopping.set(false)
            if (startPollersOnAssignment && state.started.compareAndSet(false, true)) {
                executor.submit { pollLoop(shard, state) }
            }
        }
    }

    override fun onRevoked(
        shards: Set<CoordinatorShard>,
        context: CoordinatorConsumerContext,
    ): Set<CoordinatorShard> {
        val revoked = mutableSetOf<CoordinatorShard>()
        shards.forEach { shard ->
            val state = shardStates[shard]
            if (state == null) {
                revoked += shard
                return@forEach
            }

            state.stopping.set(true)
            if (state.inFlight.get() == 0) {
                shardStates.remove(shard)
                revoked += shard
            }
        }
        return revoked
    }

    override fun onFenced(context: CoordinatorConsumerContext) {
        shardStates.values.forEach { it.stopping.set(true) }
        shardStates.clear()
    }

    fun pollOnce(shard: CoordinatorShard): Int {
        val state = shardStates[shard] ?: return 0
        if (state.stopping.get()) {
            return 0
        }

        val streamKey = shard.streamKey(properties.streamPrefix)
        val messages = reader.read(
            streamKey = streamKey,
            shard = shard,
            consumerGroup = properties.consumerGroup,
            consumerName = properties.memberId,
            count = properties.redis.pollBatchSize,
            block = properties.redis.pollTimeout,
        )

        messages.forEach { message ->
            state.inFlight.incrementAndGet()
            try {
                handler.handle(message)
                reader.ack(message.streamKey, properties.consumerGroup, message.recordId)
            } finally {
                state.inFlight.decrementAndGet()
            }
        }
        return messages.size
    }

    override fun close() {
        shardStates.values.forEach { it.stopping.set(true) }
        shardStates.clear()
        executor.shutdownNow()
    }

    private fun pollLoop(shard: CoordinatorShard, state: ShardState) {
        while (!state.stopping.get()) {
            runCatching { pollOnce(shard) }
                .onFailure { Thread.sleep(properties.redis.pollTimeout.toMillis().coerceAtLeast(1)) }
        }
        state.started.set(false)
    }

    private class ShardState {
        val stopping = AtomicBoolean(false)
        val started = AtomicBoolean(false)
        val inFlight = AtomicInteger(0)
    }
}

interface RedisStreamReader {
    fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage>

    fun ack(streamKey: String, consumerGroup: String, recordId: String)
}

class SpringDataRedisStreamReader(
    private val redisConnectionFactory: RedisConnectionFactory,
) : RedisStreamReader {
    override fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage> {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(consumerGroup.isNotBlank()) { "consumerGroup must not be blank" }
        require(consumerName.isNotBlank()) { "consumerName must not be blank" }
        require(count > 0) { "count must be positive" }

        redisConnectionFactory.connection.use { connection ->
            val records = connection.streamCommands().xReadGroup(
                Consumer.from(consumerGroup, consumerName),
                StreamReadOptions.empty().count(count).block(block),
                StreamOffset.create(streamKey.bytes(), ReadOffset.lastConsumed()),
            ).orEmpty()

            return records.map { record ->
                ConsumedRedisStreamMessage(
                    streamKey = streamKey,
                    recordId = record.id.value,
                    shard = shard,
                    fields = record.value.mapKeys { it.key.string() }.mapValues { it.value.string() },
                )
            }
        }
    }

    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
        redisConnectionFactory.connection.use { connection ->
            connection.streamCommands().xAck(streamKey.bytes(), consumerGroup, RecordId.of(recordId))
        }
    }

    private fun String.bytes(): ByteArray =
        toByteArray(Charsets.UTF_8)

    private fun ByteArray.string(): String =
        toString(Charsets.UTF_8)
}
