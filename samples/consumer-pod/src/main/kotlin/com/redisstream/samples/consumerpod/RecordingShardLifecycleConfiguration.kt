package com.redisstream.samples.consumerpod

import com.redisstream.consumer.ConsumedRedisStreamMessage
import com.redisstream.consumer.CoordinatorConsumerContext
import com.redisstream.consumer.CoordinatorConsumerProperties
import com.redisstream.consumer.CoordinatorRuntimeCapacityProvider
import com.redisstream.consumer.CoordinatorShard
import com.redisstream.consumer.CoordinatorShardLifecycle
import com.redisstream.consumer.RedisStreamAckMode
import com.redisstream.consumer.RedisStreamConsumerLifecycle
import com.redisstream.consumer.RedisStreamFailureMode
import com.redisstream.consumer.RedisStreamMessageHandler
import com.redisstream.consumer.RedisStreamReader
import com.redisstream.consumer.RedisStreamXAckDelReferencePolicy
import com.redisstream.consumer.RedisStreamXNackMode
import com.redisstream.consumer.RuntimeConsumerCapacity
import com.redisstream.consumer.SpringDataRedisStreamReader
import com.redisstream.producer.ProducerRoutingProperties
import com.redisstream.producer.RedisStreamXAddConfiguration
import com.redisstream.producer.RedisStreamWriter
import com.redisstream.producer.SpringDataRedisStreamWriter
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Duration

@Configuration(proxyBeanMethods = false)
class RecordingShardLifecycleConfiguration {
    @Bean
    fun sampleCoordinatorConsumerProperties(environment: Environment): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties.consumer(
            streamPrefix = environment.string("STREAM_PREFIX", "demo.orders"),
            consumerGroupName = environment.string("CONSUMER_GROUP_NAME", environment.string("CONSUMER_GROUP", "demo-workers")),
        ) {
            memberId = environment.string("CONSUMER_MEMBER_ID", "consumer-pod-1")
            heartbeatInterval = environment.duration("CONSUMER_HEARTBEAT_INTERVAL", Duration.ofSeconds(1))
            rebalanceTimeout = environment.duration("CONSUMER_REBALANCE_TIMEOUT", Duration.ofSeconds(10))
            runtimeMaxConcurrency = environment.int("CONSUMER_RUNTIME_MAX_CONCURRENCY", 4)
            gracefulLeaveOnStop = true
            redis.pollBatchSize = environment.long("CONSUMER_REDIS_POLL_BATCH_SIZE", 10)
            redis.pollTimeout = environment.duration("CONSUMER_REDIS_POLL_TIMEOUT", Duration.ofMillis(250))
            redis.ack.mode = environment.enum("CONSUMER_REDIS_ACK_MODE", RedisStreamAckMode.XACKDEL)
            redis.ack.xackdelReferencePolicy =
                environment.enum("CONSUMER_REDIS_XACKDEL_REFERENCE_POLICY", RedisStreamXAckDelReferencePolicy.ACKED)
            redis.failure.mode = environment.enum("CONSUMER_REDIS_FAILURE_MODE", RedisStreamFailureMode.LEAVE_PENDING)
            redis.failure.xnackMode = environment.enum("CONSUMER_REDIS_XNACK_MODE", RedisStreamXNackMode.FAIL)
            redis.failure.force = environment.boolean("CONSUMER_REDIS_XNACK_FORCE", false)
        }

    @Bean
    fun sampleProducerRoutingProperties(environment: Environment): ProducerRoutingProperties =
        ProducerRoutingProperties.producer(
            streamPrefix = environment.string("STREAM_PREFIX", "demo.orders"),
            consumerGroupName = environment.string("CONSUMER_GROUP_NAME", environment.string("CONSUMER_GROUP", "demo-workers")),
        ) {
            routingRefreshInterval = environment.duration("PRODUCER_ROUTING_REFRESH_INTERVAL", Duration.ofSeconds(2))
            publishMaxAttempts = environment.int("PRODUCER_PUBLISH_MAX_ATTEMPTS", 2)
            xadd.maxLen = environment.long("PUBLISHER_XADD_MAX_LEN", 10_000_000)
            xadd.approximateTrimming = environment.boolean("PUBLISHER_XADD_APPROXIMATE_TRIMMING", true)
        }

    @Bean
    fun sampleRedisStreamReader(
        redisConnectionFactory: RedisConnectionFactory,
        properties: CoordinatorConsumerProperties,
    ): RedisStreamReader =
        SpringDataRedisStreamReader(
            redisConnectionFactory = redisConnectionFactory,
            acknowledgement = properties.redis.ack,
            failureHandling = properties.redis.failure,
        )

    @Bean
    fun sampleRedisStreamWriter(
        redisConnectionFactory: RedisConnectionFactory,
        properties: ProducerRoutingProperties,
    ): RedisStreamWriter =
        SpringDataRedisStreamWriter(
            redisConnectionFactory = redisConnectionFactory,
            xadd = RedisStreamXAddConfiguration(
                maxLen = properties.xadd.maxLen,
                approximateTrimming = properties.xadd.approximateTrimming,
            ),
        )

    @Bean
    fun sampleRedisStreamMessageHandler(
        eventLog: ConsumerPodEventLog,
        contextHolder: ConsumerContextHolder,
    ): RedisStreamMessageHandler =
        RedisStreamMessageHandler { message ->
            val context = contextHolder.current()
            eventLog.record("message-handled", message, context)
            logger.info(
                "Handled Redis Stream message member={} shard=v{}:{} streamKey={} recordId={} fields={}",
                context.memberId,
                message.shard.streamVersion,
                message.shard.shardIndex,
                message.streamKey,
                message.recordId,
                message.fields,
            )
        }

    @Bean
    fun sampleRecordingShardLifecycle(
        properties: CoordinatorConsumerProperties,
        reader: RedisStreamReader,
        handler: RedisStreamMessageHandler,
        eventLog: ConsumerPodEventLog,
        contextHolder: ConsumerContextHolder,
    ): CoordinatorShardLifecycle =
        RecordingShardLifecycle(
            delegate = RedisStreamConsumerLifecycle(properties, reader, handler),
            eventLog = eventLog,
            contextHolder = contextHolder,
        )

    private companion object {
        val logger = LoggerFactory.getLogger(RecordingShardLifecycleConfiguration::class.java)
    }
}

private fun Environment.string(name: String, defaultValue: String): String =
    getProperty(name)?.takeIf { it.isNotBlank() } ?: defaultValue

private fun Environment.int(name: String, defaultValue: Int): Int =
    getProperty(name)?.toIntOrNull() ?: defaultValue

private fun Environment.long(name: String, defaultValue: Long): Long =
    getProperty(name)?.toLongOrNull() ?: defaultValue

private fun Environment.boolean(name: String, defaultValue: Boolean): Boolean =
    getProperty(name)?.toBooleanStrictOrNull() ?: defaultValue

private inline fun <reified T : Enum<T>> Environment.enum(name: String, defaultValue: T): T =
    getProperty(name)
        ?.let { value -> enumValues<T>().firstOrNull { it.name.equals(value, ignoreCase = true) } }
        ?: defaultValue

private fun Environment.duration(name: String, defaultValue: Duration): Duration =
    getProperty(name)?.let(::parseDuration) ?: defaultValue

private fun parseDuration(value: String): Duration {
    val trimmed = value.trim()
    return when {
        trimmed.endsWith("ms", ignoreCase = true) -> Duration.ofMillis(trimmed.dropLast(2).toLong())
        trimmed.endsWith("s", ignoreCase = true) -> Duration.ofSeconds(trimmed.dropLast(1).toLong())
        trimmed.endsWith("m", ignoreCase = true) -> Duration.ofMinutes(trimmed.dropLast(1).toLong())
        else -> Duration.parse(trimmed)
    }
}

class ConsumerContextHolder {
    @Volatile
    private var current: CoordinatorConsumerContext? = null

    fun update(context: CoordinatorConsumerContext) {
        current = context
    }

    fun current(): CoordinatorConsumerContext =
        current ?: CoordinatorConsumerContext(
            memberId = "unknown",
            memberName = "unknown",
            assignedMaxConcurrency = 0,
            metadataVersion = 0,
            groupEpoch = 0,
            assignmentEpoch = 0,
        )
}

@Configuration(proxyBeanMethods = false)
class ConsumerContextHolderConfiguration {
    @Bean
    fun consumerContextHolder(): ConsumerContextHolder =
        ConsumerContextHolder()
}

private class RecordingShardLifecycle(
    private val delegate: RedisStreamConsumerLifecycle,
    private val eventLog: ConsumerPodEventLog,
    private val contextHolder: ConsumerContextHolder,
) : CoordinatorShardLifecycle, CoordinatorRuntimeCapacityProvider, AutoCloseable {
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        contextHolder.update(context)
        eventLog.record("assigned", shards, context)
        delegate.onAssigned(shards, context)
    }

    override fun onRevoked(
        shards: Set<CoordinatorShard>,
        context: CoordinatorConsumerContext,
    ): Set<CoordinatorShard> {
        contextHolder.update(context)
        eventLog.record("revoking", shards, context)
        return delegate.onRevoked(shards, context).also { completed ->
            if (completed.isNotEmpty()) {
                eventLog.record("revoked", completed, context)
            }
        }
    }

    override fun onPending(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        contextHolder.update(context)
        eventLog.record("pending", shards, context)
        delegate.onPending(shards, context)
    }

    override fun onFenced(context: CoordinatorConsumerContext) {
        contextHolder.update(context)
        eventLog.record("fenced", emptySet(), context)
        delegate.onFenced(context)
    }

    override fun runtimeCapacity(context: CoordinatorConsumerContext): RuntimeConsumerCapacity {
        contextHolder.update(context)
        return delegate.runtimeCapacity(context)
    }

    override fun close() {
        delegate.close()
    }
}
