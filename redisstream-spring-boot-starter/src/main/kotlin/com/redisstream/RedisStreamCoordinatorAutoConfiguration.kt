package com.redisstream

import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.RestClientCoordinatorClient
import com.redisstream.consumer.coordinatorRestClient
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean

@AutoConfiguration
@EnableConfigurationProperties(RedisStreamCoordinatorProperties::class)
class RedisStreamCoordinatorAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun coordinatorClient(properties: RedisStreamCoordinatorProperties): CoordinatorClient =
        RestClientCoordinatorClient(
            coordinatorRestClient(
                coordinatorBaseUrl = properties.coordinatorBaseUrl,
                username = properties.username,
                password = properties.password,
            ),
        )
}
