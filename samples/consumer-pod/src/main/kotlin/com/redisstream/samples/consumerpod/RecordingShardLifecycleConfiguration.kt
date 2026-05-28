package com.redisstream.samples.consumerpod

import com.redisstream.consumer.ConsumedRedisStreamMessage
import com.redisstream.consumer.CoordinatorConsumerContext
import com.redisstream.consumer.CoordinatorConsumerMetrics
import com.redisstream.consumer.CoordinatorConsumerProperties
import com.redisstream.consumer.CoordinatorRuntimeCapacityProvider
import com.redisstream.consumer.CoordinatorShard
import com.redisstream.consumer.CoordinatorShardLifecycle
import com.redisstream.consumer.RedisStreamConsumerLifecycle
import com.redisstream.consumer.RedisStreamMessageHandler
import com.redisstream.consumer.RedisStreamReader
import com.redisstream.consumer.RuntimeConsumerCapacity
import com.redisstream.consumer.SpringDataRedisStreamReader
import com.redisstream.producer.RedisStreamWriter
import com.redisstream.producer.SpringDataRedisStreamWriter
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory

@Configuration(proxyBeanMethods = false)
class RecordingShardLifecycleConfiguration {
    @Bean
    fun sampleRedisStreamReader(redisConnectionFactory: RedisConnectionFactory): RedisStreamReader =
        SpringDataRedisStreamReader(redisConnectionFactory)

    @Bean
    fun sampleRedisStreamWriter(redisConnectionFactory: RedisConnectionFactory): RedisStreamWriter =
        SpringDataRedisStreamWriter(redisConnectionFactory)

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
        metrics: CoordinatorConsumerMetrics,
        eventLog: ConsumerPodEventLog,
        contextHolder: ConsumerContextHolder,
    ): CoordinatorShardLifecycle =
        RecordingShardLifecycle(
            delegate = RedisStreamConsumerLifecycle(properties, reader, handler, metrics = metrics),
            eventLog = eventLog,
            contextHolder = contextHolder,
        )

    private companion object {
        val logger = LoggerFactory.getLogger(RecordingShardLifecycleConfiguration::class.java)
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
