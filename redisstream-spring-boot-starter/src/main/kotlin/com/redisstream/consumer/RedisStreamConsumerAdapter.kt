package com.redisstream.consumer

import com.redisstream.RedisStreamCommandsTemplate
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

data class ConsumedRedisStreamMessage(
    val streamKey: String,
    val recordId: String,
    val shard: CoordinatorShard,
    val fields: Map<String, String>,
) {
    @Transient
    internal var acknowledgement: RedisStreamAcknowledgement = UnavailableRedisStreamAcknowledgement

    /**
     * Sends Redis XACK for this record. Call this only after the business side effect succeeds.
     */
    fun ack() {
        acknowledgement.ack()
    }

    /**
     * Sends Redis XACKDEL for this record. Call this only after the business side effect succeeds
     * and the application intentionally wants the stream entry deleted.
     */
    fun ackDel(referencePolicy: RedisStreamXAckDelReferencePolicy? = null) {
        acknowledgement.ackDel(referencePolicy)
    }

    /**
     * Sends Redis XNACK for this record when the connected Redis version supports it.
     */
    fun nack(
        mode: RedisStreamXNackMode? = null,
        retryCount: Long? = null,
        force: Boolean? = null,
    ) {
        acknowledgement.nack(mode, retryCount, force)
    }
}

interface RedisStreamAcknowledgement {
    /**
     * Sends Redis XACK for the current record.
     */
    fun ack()

    /**
     * Sends Redis XACKDEL for the current record.
     */
    fun ackDel(referencePolicy: RedisStreamXAckDelReferencePolicy? = null)

    /**
     * Sends Redis XNACK for the current record.
     */
    fun nack(
        mode: RedisStreamXNackMode? = null,
        retryCount: Long? = null,
        force: Boolean? = null,
    )
}

private object UnavailableRedisStreamAcknowledgement : RedisStreamAcknowledgement {
    override fun ack() {
        error("Redis Stream acknowledgement is only available for records read by RedisStreamConsumerLifecycle")
    }

    override fun ackDel(referencePolicy: RedisStreamXAckDelReferencePolicy?) {
        error("Redis Stream acknowledgement is only available for records read by RedisStreamConsumerLifecycle")
    }

    override fun nack(mode: RedisStreamXNackMode?, retryCount: Long?, force: Boolean?) {
        error("Redis Stream acknowledgement is only available for records read by RedisStreamConsumerLifecycle")
    }
}

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
    executor: Executor? = null,
) : CoordinatorShardLifecycle, CoordinatorRuntimeCapacityProvider, CoordinatorShardProgressProvider, AutoCloseable {
    private val shardStates = ConcurrentHashMap<CoordinatorShard, ShardState>()
    private val runtimeMaxConcurrency = properties.runtimeMaxConcurrency.coerceAtLeast(1)
    private val runtimePermits = Semaphore(runtimeMaxConcurrency)
    private val ownedExecutor: ExecutorService? =
        if (executor == null) Executors.newFixedThreadPool(runtimeMaxConcurrency, ::consumerThread) else null
    private val executor: Executor = executor ?: ownedExecutor!!
    private val pollingStarted = AtomicBoolean(false)
    private val nextShardCursor = AtomicInteger(0)

    /**
     * Starts polling workers for newly assigned shards.
     */
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        shards.forEach { shard ->
            val state = shardStates.computeIfAbsent(shard) { ShardState() }
            state.stopping.set(false)
        }
        if (startPollersOnAssignment) {
            startPollingWorkers()
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
        return RuntimeConsumerCapacity(
            runtimeMaxConcurrency = runtimeMaxConcurrency,
            availableConcurrency = runtimePermits.availablePermits().coerceAtLeast(0),
        )
    }

    /**
     * Reports the latest delivered and acknowledged Redis Stream ids per local shard.
     */
    override fun shardProgress(context: CoordinatorConsumerContext): List<ShardConsumptionProgress> =
        shardStates.entries
            .mapNotNull { (shard, state) -> state.progress(shard, shard.streamKey(properties.streamPrefix)) }
            .sortedBy { it.shard.shardIndex }

    /**
     * Reads and handles one batch for an assigned shard. ACK, ACKDEL, or NACK is controlled by
     * application code through the message acknowledgement helpers.
     */
    fun pollOnce(shard: CoordinatorShard): Int {
        val state = shardStates[shard] ?: return 0
        if (state.stopping.get()) {
            return 0
        }
        if (!state.polling.compareAndSet(false, true)) {
            return 0
        }

        try {
            val acquiredPermits = acquireRuntimePermits(properties.redis.pollBatchSize)
            if (acquiredPermits == 0) {
                return 0
            }

            val streamKey = shard.streamKey(properties.streamPrefix)
            val messages = try {
                reader.read(
                    streamKey = streamKey,
                    shard = shard,
                    consumerGroup = properties.consumerGroupName,
                    consumerName = properties.memberId,
                    count = acquiredPermits.toLong(),
                    block = properties.redis.pollTimeout,
                )
            } catch (error: RuntimeException) {
                runtimePermits.release(acquiredPermits)
                throw error
            }

            val processableMessages = messages.take(acquiredPermits)
            val unusedPermits = acquiredPermits - processableMessages.size
            if (unusedPermits > 0) {
                runtimePermits.release(unusedPermits)
            }

            var handledMessages = 0
            try {
                processableMessages.forEach { message ->
                    state.inFlight.incrementAndGet()
                    state.recordDelivered(message.recordId)
                    message.acknowledgement = ReaderBackedRedisStreamAcknowledgement(
                        reader = reader,
                        message = message,
                        consumerGroup = properties.consumerGroupName,
                        acknowledgement = properties.redis.ack,
                        failureHandling = properties.redis.failure,
                        state = state,
                    )
                    try {
                        handler.handle(message)
                    } finally {
                        state.inFlight.decrementAndGet()
                        runtimePermits.release()
                        handledMessages += 1
                    }
                }
            } catch (error: RuntimeException) {
                val unhandledPermits = processableMessages.size - handledMessages
                if (unhandledPermits > 0) {
                    runtimePermits.release(unhandledPermits)
                }
                throw error
            }
            return processableMessages.size
        } finally {
            state.polling.set(false)
        }
    }

    override fun close() {
        shardStates.values.forEach { it.stopping.set(true) }
        shardStates.clear()
        pollingStarted.set(false)
        ownedExecutor?.shutdownNow()
    }

    private fun consumerThread(runnable: Runnable): Thread =
        Thread(runnable, "redis-stream-consumer-${properties.memberId}").apply {
            isDaemon = true
        }

    private fun startPollingWorkers() {
        if (!pollingStarted.compareAndSet(false, true)) {
            return
        }
        repeat(runtimeMaxConcurrency) {
            executor.execute { pollLoop() }
        }
    }

    private fun pollLoop() {
        while (pollingStarted.get()) {
            val shard = nextPollableShard()
            if (shard == null) {
                Thread.sleep(properties.redis.pollTimeout.toMillis().coerceAtLeast(1))
                continue
            }
            runCatching { pollOnce(shard) }
                .onSuccess { polled ->
                    if (polled == 0) {
                        Thread.sleep(properties.redis.pollTimeout.toMillis().coerceAtLeast(1))
                    }
                }
                .onFailure { Thread.sleep(properties.redis.pollTimeout.toMillis().coerceAtLeast(1)) }
        }
    }

    private fun nextPollableShard(): CoordinatorShard? {
        // Multiple local workers can share one member, but a shard must have at most one active poll.
        val shards = shardStates.entries
            .filterNot { it.value.stopping.get() || it.value.polling.get() }
            .map { it.key }
            .sortedBy { it.shardIndex }
        if (shards.isEmpty()) {
            return null
        }
        val index = Math.floorMod(nextShardCursor.getAndIncrement(), shards.size)
        return shards[index]
    }

    private fun acquireRuntimePermits(maxCount: Long): Int {
        val limit = maxCount.coerceAtMost(Int.MAX_VALUE.toLong()).toInt()
        var acquired = 0
        while (acquired < limit && runtimePermits.tryAcquire()) {
            acquired += 1
        }
        return acquired
    }

    internal class ShardState {
        val stopping = AtomicBoolean(false)
        val polling = AtomicBoolean(false)
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

private class ReaderBackedRedisStreamAcknowledgement(
    private val reader: RedisStreamReader,
    private val message: ConsumedRedisStreamMessage,
    private val consumerGroup: String,
    private val acknowledgement: CoordinatorConsumerProperties.RedisAcknowledgement,
    private val failureHandling: CoordinatorConsumerProperties.RedisFailureHandling,
    private val state: RedisStreamConsumerLifecycle.ShardState,
) : RedisStreamAcknowledgement {
    override fun ack() {
        reader.ack(message.streamKey, consumerGroup, message.recordId)
        state.recordAcked(message.recordId)
    }

    override fun ackDel(referencePolicy: RedisStreamXAckDelReferencePolicy?) {
        reader.ackDel(
            streamKey = message.streamKey,
            consumerGroup = consumerGroup,
            recordId = message.recordId,
            referencePolicy = referencePolicy ?: acknowledgement.xackdelReferencePolicy,
        )
        state.recordAcked(message.recordId)
    }

    override fun nack(
        mode: RedisStreamXNackMode?,
        retryCount: Long?,
        force: Boolean?,
    ) {
        reader.nack(
            streamKey = message.streamKey,
            consumerGroup = consumerGroup,
            recordId = message.recordId,
            mode = mode ?: failureHandling.xnackMode,
            retryCount = retryCount ?: failureHandling.retryCount,
            force = force ?: failureHandling.force,
        )
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
     * Acknowledges and deletes a successfully handled record.
     */
    fun ackDel(
        streamKey: String,
        consumerGroup: String,
        recordId: String,
        referencePolicy: RedisStreamXAckDelReferencePolicy = RedisStreamXAckDelReferencePolicy.ACKED,
    ) {
        ack(streamKey, consumerGroup, recordId)
    }

    /**
     * Reports a failed record when the configured Redis version supports negative acknowledgement.
     */
    fun nack(
        streamKey: String,
        consumerGroup: String,
        recordId: String,
        mode: RedisStreamXNackMode = RedisStreamXNackMode.FAIL,
        retryCount: Long? = null,
        force: Boolean = false,
    ) {
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
     * Sends XACK for records explicitly acknowledged by application code.
     */
    override fun ack(streamKey: String, consumerGroup: String, recordId: String) {
        commands.xAck(streamKey, consumerGroup, recordId)
    }

    /**
     * Sends XACKDEL for records explicitly acknowledged and deleted by application code.
     */
    override fun ackDel(
        streamKey: String,
        consumerGroup: String,
        recordId: String,
        referencePolicy: RedisStreamXAckDelReferencePolicy,
    ) {
        ensureXAckDelCompatible()
        commands.xAckDel(streamKey, consumerGroup, referencePolicy.name, recordId)
    }

    /**
     * Sends XNACK for records explicitly released by application code.
     */
    override fun nack(
        streamKey: String,
        consumerGroup: String,
        recordId: String,
        mode: RedisStreamXNackMode,
        retryCount: Long?,
        force: Boolean,
    ) {
        ensureXNackCompatible()
        commands.xNack(
            streamKey = streamKey,
            consumerGroup = consumerGroup,
            mode = mode.name,
            recordId = recordId,
            retryCount = retryCount,
            force = force,
        )
    }

    /**
     * Checks Redis command support before sending XACKDEL.
     */
    private fun ensureXAckDelCompatible() {
        RedisStreamCommandCompatibility.resolveAckMode(RedisStreamAckMode.XACKDEL, commandSupportProvider.current())
    }

    /**
     * Checks Redis command support before sending XNACK.
     */
    private fun ensureXNackCompatible() {
        RedisStreamCommandCompatibility.validateFailureMode(RedisStreamFailureMode.XNACK, commandSupportProvider.current())
    }
}
