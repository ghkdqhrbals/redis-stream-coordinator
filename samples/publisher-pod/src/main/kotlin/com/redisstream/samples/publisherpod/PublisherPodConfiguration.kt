package com.redisstream.samples.publisherpod

import com.redisstream.producer.ProducerRoutingProperties
import com.redisstream.producer.RedisStreamXAddConfiguration
import com.redisstream.producer.RedisStreamWriter
import com.redisstream.producer.SpringDataRedisStreamWriter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Duration

@Configuration(proxyBeanMethods = false)
class PublisherPodConfiguration {
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
