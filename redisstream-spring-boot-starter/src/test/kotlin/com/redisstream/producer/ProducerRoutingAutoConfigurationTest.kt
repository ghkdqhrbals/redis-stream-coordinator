package com.redisstream.producer

import com.redisstream.RedisStreamCoordinatorAutoConfiguration
import com.redisstream.RedisStreamCoordinatorProperties
import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.HeartbeatRequest
import com.redisstream.consumer.HeartbeatResponse
import com.redisstream.consumer.ProducerRoutingResponse
import com.redisstream.consumer.ProducerRoutingShard
import org.mockito.Mockito
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ProducerRoutingAutoConfigurationTest {
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RedisStreamCoordinatorAutoConfiguration::class.java,
                ProducerRoutingAutoConfiguration::class.java,
            ),
        )

    @Test
    fun `producer routing cache requires code defined producer settings`() {
        contextRunner.run { context ->
            assertFalse(context.containsBean("producerRoutingCache"))
        }
    }

    @Test
    fun `producer routing cache uses the shared coordinator client bean`() {
        contextRunner
            .withBean(CoordinatorClient::class.java, {
                RoutingOnlyCoordinatorClient(routingResponse())
            })
            .withBean(ProducerRoutingProperties::class.java, {
                ProducerRoutingProperties.producer(
                    streamPrefix = "orders",
                    consumerGroupName = "orders-consumer",
                )
            })
            .withPropertyValues(
                "redis-stream-coordinator.coordinator-base-url=http://localhost:8080",
            )
            .run { context ->
                assertTrue(context.containsBean("producerRoutingCache"))
                assertEquals(1, context.getBeanNamesForType(CoordinatorClient::class.java).size)
            }
    }

    @Test
    fun `coordinator client binds optional basic auth properties`() {
        contextRunner
            .withPropertyValues(
                "redis-stream-coordinator.coordinator-base-url=http://coordinator:8080",
                "redis-stream-coordinator.username=admin",
                "redis-stream-coordinator.password=password",
            )
            .run { context ->
                val properties = context.getBean(RedisStreamCoordinatorProperties::class.java)

                assertEquals("http://coordinator:8080", properties.coordinatorBaseUrl)
                assertEquals("admin", properties.username)
                assertEquals("password", properties.password)
                assertTrue(context.containsBean("coordinatorClient"))
            }
    }

    @Test
    fun `redis stream publisher is created when redis connection factory exists`() {
        contextRunner
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(CoordinatorClient::class.java, {
                RoutingOnlyCoordinatorClient(routingResponse())
            })
            .withBean(ProducerRoutingProperties::class.java, {
                ProducerRoutingProperties.producer(
                    streamPrefix = "orders",
                    consumerGroupName = "orders-consumer",
                )
            })
            .withPropertyValues(
                "redis-stream-coordinator.coordinator-base-url=http://localhost:8080",
            )
            .run { context ->
                assertTrue(context.containsBean("redisStreamWriter"))
                assertTrue(context.containsBean("streamProducer"))
                assertEquals(
                    context.getBean(StreamProducer::class.java),
                    context.getBean(RedisStreamPublisher::class.java),
                )
            }
    }

    @Test
    fun `applications can declare multiple named stream producer beans`() {
        contextRunner
            .withUserConfiguration(NamedStreamProducerConfiguration::class.java)
            .withBean(CoordinatorClient::class.java, {
                RoutingByGroupCoordinatorClient(
                    routingResponse(streamPrefix = "orders", consumerGroup = "orders-consumer"),
                    routingResponse(streamPrefix = "payments", consumerGroup = "payments-consumer"),
                )
            })
            .withBean(RedisStreamWriter::class.java, {
                CapturingRedisStreamWriter()
            })
            .run { context ->
                assertFalse(context.containsBean("streamProducer"))
                assertTrue(context.containsBean("ordersStreamProducer"))
                assertTrue(context.containsBean("paymentsStreamProducer"))
                assertEquals(2, context.getBeanNamesForType(StreamProducer::class.java).size)
            }
    }

    @Test
    fun `producer routing cache bean fails fast when coordinator has no active shards`() {
        contextRunner
            .withBean(CoordinatorClient::class.java, {
                RoutingOnlyCoordinatorClient(routingResponse(shardCount = 0))
            })
            .withBean(ProducerRoutingProperties::class.java, {
                ProducerRoutingProperties.producer(
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

@Configuration(proxyBeanMethods = false)
private class NamedStreamProducerConfiguration {
    @Bean("ordersStreamProducer")
    fun ordersStreamProducer(
        client: CoordinatorClient,
        writer: RedisStreamWriter,
    ): StreamProducer =
        StreamProducer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = client,
            writer = writer,
        )

    @Bean("paymentsStreamProducer")
    fun paymentsStreamProducer(
        client: CoordinatorClient,
        writer: RedisStreamWriter,
    ): StreamProducer =
        StreamProducer(
            streamPrefix = "payments",
            consumerGroupName = "payments-consumer",
            client = client,
            writer = writer,
        )
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

private class RoutingByGroupCoordinatorClient(
    vararg responses: ProducerRoutingResponse,
) : CoordinatorClient {
    private val responsesByGroup = responses.associateBy { it.streamPrefix to it.consumerGroup }

    override fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse =
        error("heartbeat is not used in this test")

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        responsesByGroup[streamPrefix to consumerGroup]
            ?: error("No routing response for stream=$streamPrefix group=$consumerGroup")
}

private class CapturingRedisStreamWriter : RedisStreamWriter {
    override fun add(streamKey: String, fields: Map<String, String>): String =
        "1-0"
}

private fun routingResponse(
    streamPrefix: String = "orders",
    consumerGroup: String = "orders-consumer",
    shardCount: Int = 2,
): ProducerRoutingResponse =
    ProducerRoutingResponse(
        streamPrefix = streamPrefix,
        consumerGroup = consumerGroup,
        metadataVersion = 1,
        shardCount = shardCount,
        streamKeyPattern = "$streamPrefix:{shardIndex}",
        shards = (0 until shardCount).map { shardIndex ->
            ProducerRoutingShard(
                shardIndex = shardIndex,
                streamKey = "$streamPrefix:$shardIndex",
                redisSlot = shardIndex,
            )
        },
    )

private fun Throwable.hasCauseMessage(fragment: String): Boolean =
    generateSequence(this) { it.cause }
        .any { it.message?.contains(fragment) == true }
