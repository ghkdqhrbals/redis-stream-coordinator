package com.redisstream.consumer

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

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
    @ConditionalOnBean(CoordinatorShardLifecycle::class)
    @ConditionalOnMissingBean
    fun coordinatorManagedConsumer(
        properties: CoordinatorConsumerProperties,
        client: CoordinatorClient,
        lifecycle: CoordinatorShardLifecycle,
    ): CoordinatorManagedConsumer =
        CoordinatorManagedConsumer(properties, client, lifecycle)
}
