package com.redisstream.consumer

import com.redisstream.RedisStreamCoordinatorAutoConfiguration
import org.mockito.Mockito
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CoordinatorConsumerAutoConfigurationTest {
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RedisStreamCoordinatorAutoConfiguration::class.java,
                CoordinatorConsumerAutoConfiguration::class.java,
            ),
        )

    @Test
    fun `managed consumer is not created without code defined consumer settings`() {
        contextRunner
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
            .withBean(CoordinatorClient::class.java, {
                RoutingOnlyCoordinatorClient(routingResponse())
            })
            .withBean(CoordinatorConsumerProperties::class.java, {
                CoordinatorConsumerProperties.consumer(
                    streamPrefix = "orders",
                    consumerGroupName = "orders-consumer",
                )
            })
            .withPropertyValues(
                "redis-stream-coordinator.coordinator-base-url=http://localhost:8080",
            )
            .run { context ->
                assertTrue(context.containsBean("redisStreamReader"))
                assertTrue(context.containsBean("redisStreamConsumerLifecycle"))
                assertTrue(context.containsBean("coordinatorManagedConsumer"))
            }
    }

    @Test
    fun `managed consumer bean fails fast when coordinator has no active shards`() {
        contextRunner
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(RedisStreamMessageHandler::class.java, {
                RedisStreamMessageHandler { }
            })
            .withBean(CoordinatorClient::class.java, {
                RoutingOnlyCoordinatorClient(routingResponse(shardCount = 0))
            })
            .withBean(CoordinatorConsumerProperties::class.java, {
                CoordinatorConsumerProperties.consumer(
                    streamPrefix = "orders",
                    consumerGroupName = "orders-consumer",
                )
            })
            .run { context ->
                val failure = assertNotNull(context.startupFailure)
                assertTrue(failure.hasCauseMessage("has no active shards"))
            }
    }
}

private class RoutingOnlyCoordinatorClient(
    private val routing: ProducerRoutingResponse,
) : CoordinatorClient {
    override fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse =
        error("heartbeat is not used in this test")

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        routing
}

private fun routingResponse(
    shardCount: Int = 2,
): ProducerRoutingResponse =
    ProducerRoutingResponse(
        streamPrefix = "orders",
        consumerGroup = "orders-consumer",
        metadataVersion = 1,
        activeWriteVersion = 1,
        shardCount = shardCount,
        streamKeyPattern = "orders:v{streamVersion}:shard:{shardIndex}",
        shards = (0 until shardCount).map { shardIndex ->
            ProducerRoutingShard(
                streamVersion = 1,
                shardIndex = shardIndex,
                streamKey = "orders:v1:shard:$shardIndex",
                redisSlot = shardIndex,
            )
        },
    )

private fun Throwable.hasCauseMessage(fragment: String): Boolean =
    generateSequence(this) { it.cause }
        .any { it.message?.contains(fragment) == true }
