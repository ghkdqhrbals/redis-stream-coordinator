package com.redisstream.producer

import com.redisstream.RedisStreamCommandsTemplate
import com.redisstream.consumer.CoordinatorClient
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.data.redis.connection.RedisConnectionFactory

@AutoConfiguration(afterName = ["org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration"])
class ProducerRoutingAutoConfiguration {
    @Bean
    @ConditionalOnBean(ProducerRoutingProperties::class, CoordinatorClient::class)
    @ConditionalOnMissingBean
    fun producerRoutingCache(
        properties: ProducerRoutingProperties,
        client: CoordinatorClient,
    ): ProducerRoutingCache =
        ProducerRoutingCache(
            streamPrefix = properties.streamPrefix,
            consumerGroupName = properties.consumerGroupName,
            client = client,
            refreshInterval = properties.routingRefreshInterval,
        ).also { it.validateInitialRouting() }

    @Bean
    @ConditionalOnBean(RedisConnectionFactory::class)
    @ConditionalOnMissingBean
    fun redisStreamCommandsTemplate(redisConnectionFactory: RedisConnectionFactory): RedisStreamCommandsTemplate =
        RedisStreamCommandsTemplate(redisConnectionFactory)

    @Bean
    @ConditionalOnBean(ProducerRoutingProperties::class, RedisStreamCommandsTemplate::class)
    @ConditionalOnMissingBean
    fun redisStreamWriter(
        redisStreamCommandsTemplate: RedisStreamCommandsTemplate,
        properties: ProducerRoutingProperties,
    ): RedisStreamWriter =
        SpringDataRedisStreamWriter(
            commands = redisStreamCommandsTemplate,
            xadd = RedisStreamXAddConfiguration(
                maxLen = properties.xadd.maxLen,
                approximateTrimming = properties.xadd.approximateTrimming,
            ),
        )

    @Bean
    @ConditionalOnBean(ProducerRoutingProperties::class, ProducerRoutingCache::class, RedisStreamWriter::class)
    @ConditionalOnMissingBean
    fun redisStreamPublisher(
        properties: ProducerRoutingProperties,
        routingCache: ProducerRoutingCache,
        writer: RedisStreamWriter,
    ): RedisStreamPublisher =
        RoutingRedisStreamPublisher(
            routingCache = routingCache,
            writer = writer,
            maxAttempts = properties.publishMaxAttempts,
        )
}
