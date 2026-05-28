package com.redisstream.samples.publisherpod

import com.redisstream.producer.RedisStreamWriter
import com.redisstream.producer.SpringDataRedisStreamWriter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory

@Configuration(proxyBeanMethods = false)
class PublisherPodConfiguration {
    @Bean
    fun sampleRedisStreamWriter(redisConnectionFactory: RedisConnectionFactory): RedisStreamWriter =
        SpringDataRedisStreamWriter(redisConnectionFactory)
}
