package com.redisstream.samples.consumerpod

import com.redisstream.consumer.ConsumedRedisStreamMessage
import com.redisstream.consumer.CoordinatorConsumerContext
import com.redisstream.consumer.StreamConfiguration
import com.redisstream.consumer.StreamListener
import com.redisstream.producer.ProducerRoutingProperties
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import java.time.Duration

@StreamConfiguration
class ConsumerPodStreamListener(
    environment: Environment,
    private val eventLog: ConsumerPodEventLog,
) {
    private val processingDelay = environment.duration("CONSUMER_PROCESSING_DELAY", Duration.ofMillis(300))
    private val consumerGroupName = environment.string(
        "CONSUMER_GROUP_NAME",
        environment.string("CONSUMER_GROUP", "demo-workers"),
    )
    private val secondaryConsumerGroupName = environment.string(
        "SECONDARY_CONSUMER_GROUP_NAME",
        "payment-workers",
    )
    private val lowVolumePaymentConsumerGroupName = environment.string(
        "PAYMENT_LOW_CONSUMER_GROUP_NAME",
        "payment-low-workers",
    )
    private val listenerConcurrency = environment.int(
        "CONSUMER_MEMBER_CONCURRENCY",
        environment.int("CONSUMER_RUNTIME_MAX_CONCURRENCY", 4),
    )
    private val secondaryListenerConcurrency = environment.int(
        "SECONDARY_CONSUMER_MEMBER_CONCURRENCY",
        listenerConcurrency,
    )
    private val lowVolumePaymentListenerConcurrency = environment.int(
        "PAYMENT_LOW_CONSUMER_MEMBER_CONCURRENCY",
        1,
    )

    @StreamListener(
        id = "consumer-pod-listener",
        streamPrefix = "\${STREAM_PREFIX:create-order}",
        groupId = "\${CONSUMER_GROUP_NAME:\${CONSUMER_GROUP:demo-workers}}",
        concurrency = "\${CONSUMER_MEMBER_CONCURRENCY:\${CONSUMER_RUNTIME_MAX_CONCURRENCY:4}}",
        pollBatchSize = "\${CONSUMER_REDIS_POLL_BATCH_SIZE:10}",
        pollTimeoutMs = "\${CONSUMER_REDIS_POLL_TIMEOUT_MS:250}",
    )
    fun consume(message: ConsumedRedisStreamMessage) {
        handle(message, consumerGroupName, listenerConcurrency)
    }

    @StreamListener(
        id = "consumer-pod-secondary-listener",
        streamPrefix = "\${SECONDARY_STREAM_PREFIX:create-payment}",
        groupId = "\${SECONDARY_CONSUMER_GROUP_NAME:payment-workers}",
        concurrency = "\${SECONDARY_CONSUMER_MEMBER_CONCURRENCY:\${CONSUMER_MEMBER_CONCURRENCY:\${CONSUMER_RUNTIME_MAX_CONCURRENCY:4}}}",
        autoStartup = "\${SECONDARY_STREAM_ENABLED:false}",
        pollBatchSize = "\${CONSUMER_REDIS_POLL_BATCH_SIZE:10}",
        pollTimeoutMs = "\${CONSUMER_REDIS_POLL_TIMEOUT_MS:250}",
    )
    fun consumeSecondary(message: ConsumedRedisStreamMessage) {
        handle(message, secondaryConsumerGroupName, secondaryListenerConcurrency)
    }

    @StreamListener(
        id = "consumer-pod-payment-low-listener",
        streamPrefix = "\${PAYMENT_LOW_STREAM_PREFIX:create-payment}",
        groupId = "\${PAYMENT_LOW_CONSUMER_GROUP_NAME:payment-low-workers}",
        concurrency = "\${PAYMENT_LOW_CONSUMER_MEMBER_CONCURRENCY:1}",
        autoStartup = "\${PAYMENT_LOW_STREAM_ENABLED:false}",
        pollBatchSize = "\${CONSUMER_REDIS_POLL_BATCH_SIZE:10}",
        pollTimeoutMs = "\${CONSUMER_REDIS_POLL_TIMEOUT_MS:250}",
    )
    fun consumeLowVolumePayment(message: ConsumedRedisStreamMessage) {
        handle(message, lowVolumePaymentConsumerGroupName, lowVolumePaymentListenerConcurrency)
    }

    private fun handle(
        message: ConsumedRedisStreamMessage,
        consumerGroupName: String,
        listenerConcurrency: Int,
    ) {
        if (!processingDelay.isZero && !processingDelay.isNegative) {
            Thread.sleep(processingDelay.toMillis())
        }
        val context = sampleContext(consumerGroupName, listenerConcurrency)
        eventLog.record("message-handled", message, context)
        logger.info(
            "Handled Redis Stream message member={} shard={} streamKey={} recordId={} fields={}",
            context.memberId,
            message.shard.shardIndex,
            message.streamKey,
            message.recordId,
            message.fields,
        )
        message.ack()
    }

    private fun sampleContext(
        consumerGroupName: String,
        listenerConcurrency: Int,
    ): CoordinatorConsumerContext =
        CoordinatorConsumerContext(
            memberId = "auto-generated",
            memberName = consumerGroupName,
            assignedMaxConcurrency = listenerConcurrency,
            metadataVersion = 0,
            groupEpoch = 0,
            assignmentEpoch = 0,
        )

    private companion object {
        val logger = LoggerFactory.getLogger(ConsumerPodStreamListener::class.java)
    }
}

@Configuration(proxyBeanMethods = false)
class ConsumerPodRuntimeConfiguration {
    @Bean
    fun sampleProducerRoutingProperties(environment: Environment): ProducerRoutingProperties =
        ProducerRoutingProperties.producer(
            streamPrefix = environment.string("STREAM_PREFIX", "create-order"),
            consumerGroupName = environment.string(
                "CONSUMER_GROUP_NAME",
                environment.string("CONSUMER_GROUP", "demo-workers"),
            ),
        ) {
            routingRefreshInterval = environment.duration("PRODUCER_ROUTING_REFRESH_INTERVAL", Duration.ofSeconds(2))
            publishMaxAttempts = environment.int("PRODUCER_PUBLISH_MAX_ATTEMPTS", 2)
            xadd.maxLen = environment.long("PUBLISHER_XADD_MAX_LEN", 10_000_000)
            xadd.approximateTrimming = environment.boolean("PUBLISHER_XADD_APPROXIMATE_TRIMMING", true)
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
