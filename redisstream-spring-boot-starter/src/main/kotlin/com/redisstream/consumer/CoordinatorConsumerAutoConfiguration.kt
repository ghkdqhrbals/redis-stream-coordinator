package com.redisstream.consumer

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.data.redis.connection.RedisConnectionFactory

@AutoConfiguration
@EnableConfigurationProperties(CoordinatorConsumerProperties::class)
@ConditionalOnProperty(
    prefix = "redis-stream-coordinator.consumer",
    name = ["enabled"],
    havingValue = "true",
    matchIfMissing = true,
)
class CoordinatorConsumerAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun coordinatorClient(properties: CoordinatorConsumerProperties): CoordinatorClient =
        RestClientCoordinatorClient(coordinatorRestClient(properties))

    @Bean
    @ConditionalOnBean(RedisConnectionFactory::class)
    @ConditionalOnMissingBean
    fun redisStreamReader(redisConnectionFactory: RedisConnectionFactory): RedisStreamReader =
        SpringDataRedisStreamReader(redisConnectionFactory)

    @Bean
    @ConditionalOnBean(RedisStreamMessageHandler::class, RedisStreamReader::class)
    @ConditionalOnMissingBean(CoordinatorShardLifecycle::class)
    @ConditionalOnProperty(
        prefix = "redis-stream-coordinator.consumer.redis",
        name = ["enabled"],
        havingValue = "true",
    )
    fun redisStreamConsumerLifecycle(
        properties: CoordinatorConsumerProperties,
        reader: RedisStreamReader,
        handler: RedisStreamMessageHandler,
    ): CoordinatorShardLifecycle =
        RedisStreamConsumerLifecycle(properties, reader, handler)

    @Bean
    @ConditionalOnBean(CoordinatorShardLifecycle::class)
    @ConditionalOnMissingBean
    fun coordinatorManagedConsumer(
        properties: CoordinatorConsumerProperties,
        client: CoordinatorClient,
        lifecycle: CoordinatorShardLifecycle,
    ): CoordinatorManagedConsumer =
        CoordinatorManagedConsumer(properties, client, lifecycle)
}
