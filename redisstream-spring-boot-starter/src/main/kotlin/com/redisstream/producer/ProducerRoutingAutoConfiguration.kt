package com.redisstream.producer

import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.RestClientCoordinatorClient
import com.redisstream.consumer.coordinatorRestClient
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.data.redis.connection.RedisConnectionFactory

@AutoConfiguration
@EnableConfigurationProperties(ProducerRoutingProperties::class)
@ConditionalOnProperty(
    prefix = "redis-stream-coordinator.producer",
    name = ["enabled"],
    havingValue = "true",
)
class ProducerRoutingAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun redisStreamProducerMetrics(
        properties: ProducerRoutingProperties,
        meterRegistry: ObjectProvider<MeterRegistry>,
    ): RedisStreamProducerMetrics =
        meterRegistry.ifAvailable?.let {
            MicrometerRedisStreamProducerMetrics(
                registry = it,
                streamPrefix = properties.streamPrefix,
                consumerGroup = properties.consumerGroup,
            )
        } ?: NoopRedisStreamProducerMetrics

    @Bean
    @ConditionalOnMissingBean
    fun producerRoutingCache(
        properties: ProducerRoutingProperties,
        metrics: RedisStreamProducerMetrics,
    ): ProducerRoutingCache {
        val client: CoordinatorClient = RestClientCoordinatorClient(
            coordinatorRestClient(
                coordinatorBaseUrl = properties.coordinatorBaseUrl,
                username = properties.username,
                password = properties.password,
            ),
        )
        return ProducerRoutingCache(
            streamPrefix = properties.streamPrefix,
            consumerGroup = properties.consumerGroup,
            client = client,
            refreshInterval = properties.routingRefreshInterval,
            metrics = metrics,
        )
    }

    @Bean
    @ConditionalOnBean(RedisConnectionFactory::class)
    @ConditionalOnMissingBean
    fun redisStreamWriter(redisConnectionFactory: RedisConnectionFactory): RedisStreamWriter =
        SpringDataRedisStreamWriter(redisConnectionFactory)

    @Bean
    @ConditionalOnBean(RedisStreamWriter::class)
    @ConditionalOnMissingBean
    fun redisStreamPublisher(
        properties: ProducerRoutingProperties,
        routingCache: ProducerRoutingCache,
        writer: RedisStreamWriter,
        metrics: RedisStreamProducerMetrics,
    ): RedisStreamPublisher =
        RoutingRedisStreamPublisher(
            routingCache = routingCache,
            writer = writer,
            metrics = metrics,
            maxAttempts = properties.publishMaxAttempts,
        )
}
