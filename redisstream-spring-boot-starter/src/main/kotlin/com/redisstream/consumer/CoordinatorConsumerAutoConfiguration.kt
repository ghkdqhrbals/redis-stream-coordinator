package com.redisstream.consumer

import com.redisstream.RedisStreamCommandsTemplate
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.data.redis.connection.RedisConnectionFactory

@AutoConfiguration
class CoordinatorConsumerAutoConfiguration {
    @Bean
    @ConditionalOnBean(RedisConnectionFactory::class)
    @ConditionalOnMissingBean
    fun redisStreamCommandsTemplate(redisConnectionFactory: RedisConnectionFactory): RedisStreamCommandsTemplate =
        RedisStreamCommandsTemplate(redisConnectionFactory)

    @Bean
    @ConditionalOnBean(CoordinatorConsumerProperties::class, RedisStreamCommandsTemplate::class)
    @ConditionalOnMissingBean
    fun redisStreamReader(
        redisStreamCommandsTemplate: RedisStreamCommandsTemplate,
        properties: CoordinatorConsumerProperties,
    ): RedisStreamReader =
        SpringDataRedisStreamReader(
            commands = redisStreamCommandsTemplate,
            acknowledgement = properties.redis.ack,
            failureHandling = properties.redis.failure,
        )

    @Bean
    @ConditionalOnBean(
        CoordinatorConsumerProperties::class,
        RedisStreamMessageHandler::class,
        RedisStreamReader::class,
    )
    @ConditionalOnMissingBean(CoordinatorShardLifecycle::class)
    fun redisStreamConsumerLifecycle(
        properties: CoordinatorConsumerProperties,
        reader: RedisStreamReader,
        handler: RedisStreamMessageHandler,
    ): CoordinatorShardLifecycle =
        RedisStreamConsumerLifecycle(properties, reader, handler)

    @Bean
    @ConditionalOnBean(CoordinatorConsumerProperties::class, CoordinatorShardLifecycle::class)
    @ConditionalOnMissingBean
    fun coordinatorManagedConsumer(
        properties: CoordinatorConsumerProperties,
        client: CoordinatorClient,
        lifecycle: CoordinatorShardLifecycle,
    ): CoordinatorManagedConsumer =
        CoordinatorManagedConsumer(properties, client, lifecycle)
            .also { it.validateInitialRouting() }
}
