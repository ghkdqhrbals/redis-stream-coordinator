package com.redisstream.consumer

import org.mockito.Mockito
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CoordinatorConsumerAutoConfigurationTest {
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(CoordinatorConsumerAutoConfiguration::class.java))

    @Test
    fun `managed consumer is not created without shard lifecycle or redis message handler`() {
        contextRunner
            .withPropertyValues(
                "redis-stream-coordinator.consumer.stream-prefix=orders",
                "redis-stream-coordinator.consumer.consumer-group=orders-consumer",
            )
            .run { context ->
                assertFalse(context.containsBean("coordinatorManagedConsumer"))
            }
    }

    @Test
    fun `redis message handler enables built in stream consumer lifecycle`() {
        contextRunner
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(RedisStreamMessageHandler::class.java, {
                RedisStreamMessageHandler { }
            })
            .withPropertyValues(
                "redis-stream-coordinator.consumer.stream-prefix=orders",
                "redis-stream-coordinator.consumer.consumer-group=orders-consumer",
                "redis-stream-coordinator.consumer.redis.enabled=true",
            )
            .run { context ->
                assertTrue(context.containsBean("redisStreamReader"))
                assertTrue(context.containsBean("redisStreamConsumerLifecycle"))
                assertTrue(context.containsBean("coordinatorManagedConsumer"))
            }
    }
}
