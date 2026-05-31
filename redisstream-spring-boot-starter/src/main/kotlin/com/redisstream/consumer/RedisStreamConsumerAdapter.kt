package com.redisstream.consumer

import com.redisstream.RedisStreamCommandsTemplate
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Duration
import java.time.Instant
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
    /**
     * Handles one Redis Stream record after it has been routed to an assigned shard.
     */
    fun handle(message: ConsumedRedisStreamMessage)
}

class RedisStreamConsumerLifecycle(
    private val properties: CoordinatorConsumerProperties,
    private val reader: RedisStreamReader,
    private val handler: RedisStreamMessageHandler,
    private val startPollersOnAssignment: Boolean = true,
) : CoordinatorShardLifecycle, CoordinatorRuntimeCapacityProvider, CoordinatorShardProgressProvider, AutoCloseable {
    private val shardStates = ConcurrentHashMap<CoordinatorShard, ShardState>()
    private val executor: ExecutorService = Executors.newCachedThreadPool { runnable ->
        Thread(runnable, "redis-stream-consumer-${properties.memberId}").apply {
            isDaemon = true
        }
    }

    /**
     * Starts polling workers for newly assigned shards.
     */
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        shards.forEach { shard ->
            val state = shardStates.computeIfAbsent(shard) { ShardState() }
            state.stopping.set(false)
            if (startPollersOnAssignment && state.started.compareAndSet(false, true)) {
                executor.submit { pollLoop(shard, state) }
            }
        }
    }

    /**
     * Stops polling revoked shards and reports only shards with no in-flight handler calls.
     */
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

    /**
     * Immediately drops all local shard state after coordinator fencing.
     */
    override fun onFenced(context: CoordinatorConsumerContext) {
        shardStates.values.forEach { it.stopping.set(true) }
        shardStates.clear()
    }

    /**
     * Reports available local concurrency based on currently in-flight handler calls.
     */
    override fun runtimeCapacity(context: CoordinatorConsumerContext): RuntimeConsumerCapacity {
        val inFlight = shardStates.values.sumOf { it.inFlight.get() }
        val runtimeMaxConcurrency = properties.runtimeMaxConcurrency.coerceAtLeast(1)
        return RuntimeConsumerCapacity(
            runtimeMaxConcurrency = runtimeMaxConcurrency,
            availableConcurrency = (runtimeMaxConcurrency - inFlight).coerceAtLeast(0),
        )
    }

    /**
     * Reports the latest delivered and acknowledged Redis Stream ids per local shard.
     */
    override fun shardProgress(context: CoordinatorConsumerContext): List<ShardConsumptionProgress> =
        shardStates.entries
            .mapNotNull { (shard, state) -> state.progress(shard, shard.streamKey(properties.streamPrefix)) }
            .sortedWith(compareBy<ShardConsumptionProgress> { it.shard.streamVersion }.thenBy { it.shard.shardIndex })

    /**
     * Reads, handles, and acknowledges one batch for an assigned shard.
     */
    fun pollOnce(shard: CoordinatorShard): Int {
        val state = shardStates[shard] ?: return 0
        if (state.stopping.get()) {
            return 0
        }

        val streamKey = shard.streamKey(properties.streamPrefix)
        val messages = reader.read(
            streamKey = streamKey,
            shard = shard,
            consumerGroup = properties.consumerGroupName,
            consumerName = properties.memberId,
            count = properties.redis.pollBatchSize,
            block = properties.redis.pollTimeout,
        )

        messages.forEach { message ->
            state.inFlight.incrementAndGet()
            state.recordDelivered(message.recordId)
            try {
                try {
                    handler.handle(message)
                } catch (error: RuntimeException) {
                    if (properties.redis.failure.mode == RedisStreamFailureMode.XNACK) {
                        runCatching {
                            reader.nack(message.streamKey, properties.consumerGroupName, message.recordId)
                        }.onFailure(error::addSuppressed)
                    }
                    throw error
                }

                try {
                    reader.ack(message.streamKey, properties.consumerGroupName, message.recordId)
                    state.recordAcked(message.recordId)
                } catch (error: RuntimeException) {
                    throw error
                }
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
        @Volatile
        private var lastDeliveredId: String? = null
        @Volatile
        private var lastAckedId: String? = null
        @Volatile
        private var updatedAt: Instant? = null

        fun recordDelivered(recordId: String) {
            lastDeliveredId = recordId
            updatedAt = Instant.now()
        }

        fun recordAcked(recordId: String) {
            lastAckedId = recordId
            updatedAt = Instant.now()
        }

        fun progress(shard: CoordinatorShard, streamKey: String): ShardConsumptionProgress? {
            val delivered = lastDeliveredId
            val acked = lastAckedId
            if (delivered == null && acked == null) {
                return null
            }
            return ShardConsumptionProgress(
                shard = shard,
                streamKey = streamKey,
                lastDeliveredId = delivered,
                lastAckedId = acked,
                pendingCount = inFlight.get().toLong(),
                updatedAt = updatedAt,
            )
        }
    }
}

interface RedisStreamReader {
    /**
     * Reads records for one stream shard using a Redis consumer group.
     */
    fun read(
        streamKey: String,
        shard: CoordinatorShard,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<ConsumedRedisStreamMessage>

    /**
     * Acknowledges a successfully handled record.
     */
    fun ack(streamKey: String, consumerGroup: String, recordId: String)

    /**
     * Reports a failed record when the configured Redis version supports negative acknowledgement.
     */
    fun nack(streamKey: String, consumerGroup: String, recordId: String) {
    }
}

class SpringDataRedisStreamReader(
    private val commands: RedisStreamCommandsTemplate,
    private val acknowledgement: CoordinatorConsumerProperties.RedisAcknowledgement = CoordinatorConsumerProperties.RedisAcknowledgement(),
    private val failureHandling: CoordinatorConsumerProperties.RedisFailureHandling = CoordinatorConsumerProperties.RedisFailureHandling(),
    private val commandSupportProvider: RedisStreamCommandSupportProvider =
        RedisConnectionStreamCommandSupportProvider(commands),
) : RedisStreamReader {
    constructor(
        redisConnectionFactory: RedisConnectionFactory,
        acknowledgement: CoordinatorConsumerProperties.RedisAcknowledgement = CoordinatorConsumerProperties.RedisAcknowledgement(),
        failureHandling: CoordinatorConsumerProperties.RedisFailureHandling = CoordinatorConsumerProperties.RedisFailureHandling(),
        commandSupportProvider: RedisStreamCommandSupportProvider =
            RedisConnectionStreamCommandSupportProvider(redisConnectionFactory),
    ) : this(
        RedisStreamCommandsTemplate(redisConnectionFactory),
        acknowledgement,
        failureHandling,
        commandSupportProvider,
    )

    @Volatile
    private var resolvedAckMode: RedisStreamResolvedAckMode? = null

    /**
     * Executes XREADGROUP and converts raw Redis records into shard-aware consumer messages.
     */
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

        return commands.xReadGroup(streamKey, consumerGroup, consumerName, count, block)
            .map { record ->
                ConsumedRedisStreamMessage(
                    streamKey = streamKey,
                    recordId = record.id,
                    shard = shard,
                    fields = record.fields,
                )
            }
    }

    /**
     * Sends XACK or XACKDEL according to configured acknowledgement mode and Redis version support.
     */
    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
        ensureCompatible()
        val ackMode = resolvedAckMode()
        when (ackMode) {
            RedisStreamResolvedAckMode.XACK ->
                commands.xAck(streamKey, consumerGroup, recordId)
            RedisStreamResolvedAckMode.XACKDEL ->
                commands.xAckDel(streamKey, consumerGroup, acknowledgement.xackdelReferencePolicy.name, recordId)
        }
    }

    /**
     * Sends XNACK for failed records when failure handling is explicitly enabled.
     */
    override fun nack(streamKey: String, consumerGroup: String, recordId: String) {
        if (failureHandling.mode != RedisStreamFailureMode.XNACK) {
            return
        }
        ensureCompatible()
        commands.xNack(
            streamKey = streamKey,
            consumerGroup = consumerGroup,
            mode = failureHandling.xnackMode.name,
            recordId = recordId,
            retryCount = failureHandling.retryCount,
            force = failureHandling.force,
        )
    }

    /**
     * Loads command support before sending newer Redis Stream commands.
     */
    private fun ensureCompatible() {
        val support = commandSupportProvider.current()
        resolvedAckMode ?: RedisStreamCommandCompatibility
            .resolveAckMode(acknowledgement.mode, support)
            .also { resolvedAckMode = it }
        RedisStreamCommandCompatibility.validateFailureMode(failureHandling.mode, support)
    }

    private fun resolvedAckMode(): RedisStreamResolvedAckMode =
        resolvedAckMode ?: synchronized(this) {
            resolvedAckMode ?: RedisStreamCommandCompatibility
                .resolveAckMode(acknowledgement.mode, commandSupportProvider.current())
                .also { resolvedAckMode = it }
        }
}
