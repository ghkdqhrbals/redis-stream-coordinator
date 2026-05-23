package com.redisstream.producer

import com.redisstream.consumer.CoordinatorClient
import org.mockito.Mockito
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProducerRoutingAutoConfigurationTest {
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ProducerRoutingAutoConfiguration::class.java))

    @Test
    fun `producer routing cache is disabled by default`() {
        contextRunner.run { context ->
            assertFalse(context.containsBean("producerRoutingCache"))
        }
    }

    @Test
    fun `producer routing cache can be enabled without exporting a shared coordinator client bean`() {
        contextRunner
            .withPropertyValues(
                "redis-stream-coordinator.producer.enabled=true",
                "redis-stream-coordinator.producer.coordinator-base-url=http://localhost:8080",
                "redis-stream-coordinator.producer.stream-prefix=orders",
                "redis-stream-coordinator.producer.consumer-group=orders-consumer",
            )
            .run { context ->
                assertTrue(context.containsBean("producerRoutingCache"))
                assertEquals(0, context.getBeanNamesForType(CoordinatorClient::class.java).size)
            }
    }

    @Test
    fun `redis stream publisher is created when redis connection factory exists`() {
        contextRunner
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withPropertyValues(
                "redis-stream-coordinator.producer.enabled=true",
                "redis-stream-coordinator.producer.coordinator-base-url=http://localhost:8080",
                "redis-stream-coordinator.producer.stream-prefix=orders",
                "redis-stream-coordinator.producer.consumer-group=orders-consumer",
            )
            .run { context ->
                assertTrue(context.containsBean("redisStreamWriter"))
                assertTrue(context.containsBean("redisStreamPublisher"))
            }
    }
}
