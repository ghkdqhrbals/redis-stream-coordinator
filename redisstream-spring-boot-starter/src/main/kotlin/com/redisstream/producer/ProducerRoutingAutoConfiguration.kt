package com.redisstream.producer

import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.RestClientCoordinatorClient
import com.redisstream.consumer.coordinatorRestClient
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

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
    fun producerRoutingCache(properties: ProducerRoutingProperties): ProducerRoutingCache {
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
        )
    }
}
