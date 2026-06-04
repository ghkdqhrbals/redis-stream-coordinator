package com.redisstream.consumer

import com.redisstream.RedisStreamCoordinatorAutoConfiguration
import org.mockito.Mockito
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.SmartLifecycle
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNotEquals
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
            .withBean("ordersExecutor", Executor::class.java, {
                Executors.newSingleThreadExecutor()
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
                assertTrue(context.containsBean("coordinatorManagedConsumer"))
            }
    }

    @Test
    fun `managed consumer is created even when another smart lifecycle bean exists`() {
        contextRunner
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(RedisStreamMessageHandler::class.java, {
                RedisStreamMessageHandler { }
            })
            .withBean(CoordinatorClient::class.java, {
                RecordingCoordinatorClient(routingResponse())
            })
            .withBean("otherSmartLifecycle", SmartLifecycle::class.java, {
                NoopSmartLifecycle()
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
                assertTrue(context.containsBean("otherSmartLifecycle"))
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

    @Test
    fun `stream annotations create consumer properties and listener handler`() {
        contextRunner
            .withUserConfiguration(AnnotatedOrderConsumer::class.java)
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(CoordinatorClient::class.java, {
                RoutingOnlyCoordinatorClient(routingResponse())
            })
            .withBean("ordersExecutor", Executor::class.java, {
                Executors.newSingleThreadExecutor()
            })
            .withPropertyValues(
                "test.stream-prefix=orders",
                "test.listener-concurrency=6",
            )
            .run { context ->
                assertTrue(context.containsBean("streamListenerConsumerDefinitions"))
                assertFalse(context.containsBean("streamConfiguredCoordinatorConsumerProperties"))
                assertFalse(context.containsBean("streamListenerMessageHandler"))

                val definitions = context.getBean(StreamListenerConsumerDefinitions::class.java)
                assertEquals(1, definitions.definitions.size)
                val definition = definitions.definitions.single()
                val properties = definition.properties
                assertEquals("orders", properties.streamPrefix)
                assertEquals("orders-consumer", properties.consumerGroupName)
                assertTrue(properties.memberId.isNotBlank())
                assertNotEquals("listener-a", properties.memberId)
                assertEquals(1, properties.runtimeMaxConcurrency)
                assertEquals(6, properties.memberCount)
                assertEquals("ordersExecutor", properties.executorBeanName)
                assertEquals(5, properties.redis.pollBatchSize)
                assertEquals(Duration.ofMillis(250), properties.redis.pollTimeout)

                val listener = context.getBean(AnnotatedOrderConsumer::class.java)
                definition.handler.handle(
                    ConsumedRedisStreamMessage(
                        streamKey = "orders:0",
                        recordId = "1-0",
                        shard = CoordinatorShard(0),
                        fields = mapOf("payload" to "created"),
                    ),
                )
                assertEquals(listOf("created"), listener.handledPayloads)
            }
    }

    @Test
    fun `runtime identity prefers pod ip context`() {
        val memberId = ConsumerRuntimeIdentity.defaultMemberId(
            environment = mapOf(
                "POD_IP" to "10.244.1.25",
                "HOSTNAME" to "orders-consumer-5fcd9b7b89-zk7p9",
            ),
            localAddress = { "172.18.0.10" },
        )

        assertEquals("10.244.1.25", memberId)
    }

    @Test
    fun `runtime identity falls back to sanitized local address`() {
        val memberId = ConsumerRuntimeIdentity.defaultMemberId(
            environment = mapOf("HOSTNAME" to "orders-consumer"),
            localAddress = { "fd00::1" },
        )

        assertEquals("fd00--1", memberId)
    }

    @Test
    fun `member split uses pod ip based member id suffixes`() {
        val properties = CoordinatorConsumerProperties.consumer("orders", "orders-consumer") {
            memberId = ConsumerRuntimeIdentity.defaultMemberId(
                environment = mapOf("POD_IP" to "10.244.1.25"),
                localAddress = { "172.18.0.10" },
            )
            memberCount = 3
        }

        assertEquals("10.244.1.25-m0", properties.copyForMember(0).memberId)
        assertEquals("10.244.1.25-m1", properties.copyForMember(1).memberId)
        assertEquals("10.244.1.25-m2", properties.copyForMember(2).memberId)
    }

    @Test
    fun `stream listener concurrency starts independent coordinator members`() {
        val client = RecordingCoordinatorClient(routingResponse())

        contextRunner
            .withUserConfiguration(AnnotatedOrderConsumer::class.java)
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(CoordinatorClient::class.java, {
                client
            })
            .withBean("ordersExecutor", Executor::class.java, {
                Executors.newSingleThreadExecutor()
            })
            .withPropertyValues(
                "test.stream-prefix=orders",
                "test.listener-concurrency=4",
            )
            .run { context ->
                assertTrue(waitUntil { client.requests.map { it.memberId }.toSet().size == 4 })

                val memberIds = client.requests.map { it.memberId }.toSet()
                assertEquals(4, memberIds.size)
                assertEquals(setOf("-m0", "-m1", "-m2", "-m3"), memberIds.map { it.takeLast(3) }.toSet())
                assertTrue(client.requests.all { it.request.runtimeConsumerCapacity.runtimeMaxConcurrency == 1 })

                context.getBean("coordinatorManagedConsumer", SmartLifecycle::class.java).stop()
            }
    }

    @Test
    fun `stream listener can inherit class level stream identity defaults`() {
        contextRunner
            .withUserConfiguration(DefaultedOrderConsumer::class.java)
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(CoordinatorClient::class.java, {
                RoutingOnlyCoordinatorClient(routingResponse())
            })
            .withPropertyValues(
                "test.default-stream-prefix=orders",
            )
            .run { context ->
                val properties = context.getBean(StreamListenerConsumerDefinitions::class.java)
                    .definitions
                    .single()
                    .properties
                assertEquals("orders", properties.streamPrefix)
                assertEquals("orders-consumer", properties.consumerGroupName)
                assertTrue(properties.memberId.isNotBlank())
                assertEquals(1, properties.runtimeMaxConcurrency)
                assertEquals(2, properties.memberCount)
            }
    }

    @Test
    fun `multiple stream listeners create independent coordinator members in one application context`() {
        val client = RecordingCoordinatorClient(routingResponse())

        contextRunner
            .withUserConfiguration(AnnotatedMultiStreamConsumer::class.java)
            .withBean(RedisConnectionFactory::class.java, {
                Mockito.mock(RedisConnectionFactory::class.java)
            })
            .withBean(CoordinatorClient::class.java, {
                client
            })
            .withPropertyValues(
                "test.orders-concurrency=2",
                "test.payments-concurrency=3",
            )
            .run { context ->
                val definitions = context.getBean(StreamListenerConsumerDefinitions::class.java)
                assertEquals(2, definitions.definitions.size)
                assertEquals(
                    setOf("orders", "payments"),
                    definitions.definitions.map { it.properties.streamPrefix }.toSet(),
                )

                val listener = context.getBean(AnnotatedMultiStreamConsumer::class.java)
                definitions.definitions.single { it.properties.streamPrefix == "orders" }.handler.handle(
                    ConsumedRedisStreamMessage(
                        streamKey = "orders:0",
                        recordId = "1-0",
                        shard = CoordinatorShard(0),
                        fields = mapOf("payload" to "order-created"),
                    ),
                )
                definitions.definitions.single { it.properties.streamPrefix == "payments" }.handler.handle(
                    ConsumedRedisStreamMessage(
                        streamKey = "payments:0",
                        recordId = "2-0",
                        shard = CoordinatorShard(0),
                        fields = mapOf("payload" to "payment-created"),
                    ),
                )
                assertEquals(listOf("order-created"), listener.orders)
                assertEquals(listOf("payment-created"), listener.payments)

                assertTrue(
                    waitUntil {
                        client.requests.count { it.streamPrefix == "orders" } >= 2 &&
                            client.requests.count { it.streamPrefix == "payments" } >= 3
                    },
                )
                assertEquals(2, client.requests.filter { it.streamPrefix == "orders" }.map { it.memberId }.toSet().size)
                assertEquals(3, client.requests.filter { it.streamPrefix == "payments" }.map { it.memberId }.toSet().size)

                context.getBean("coordinatorManagedConsumer", SmartLifecycle::class.java).stop()
            }
    }

}

@StreamConfiguration(
    executor = "ordersExecutor",
    pollBatchSize = 5,
    pollTimeoutMs = 250,
)
class AnnotatedOrderConsumer {
    val handledPayloads = mutableListOf<String>()

    @StreamListener(
        id = "listener-a",
        streamPrefix = "\${test.stream-prefix}",
        groupId = "orders-consumer",
        concurrency = "\${test.listener-concurrency}",
    )
    fun listen(message: ConsumedRedisStreamMessage) {
        handledPayloads += message.fields.getValue("payload")
    }
}

@StreamConfiguration(
    streamPrefix = "\${test.default-stream-prefix}",
    consumerGroupName = "orders-consumer",
)
class DefaultedOrderConsumer {
    @StreamListener(concurrency = "2")
    fun listen(message: ConsumedRedisStreamMessage) {
    }
}

@StreamConfiguration
class AnnotatedMultiStreamConsumer {
    val orders = mutableListOf<String>()
    val payments = mutableListOf<String>()

    @StreamListener(
        streamPrefix = "orders",
        groupId = "orders-consumer",
        concurrency = "\${test.orders-concurrency}",
    )
    fun listenOrder(message: ConsumedRedisStreamMessage) {
        orders += message.fields.getValue("payload")
    }

    @StreamListener(
        streamPrefix = "payments",
        groupId = "payments-consumer",
        concurrency = "\${test.payments-concurrency}",
    )
    fun listenPayment(message: ConsumedRedisStreamMessage) {
        payments += message.fields.getValue("payload")
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
        routing.forGroup(streamPrefix, consumerGroup)
}

private data class RecordedHeartbeat(
    val streamPrefix: String,
    val consumerGroup: String,
    val memberId: String,
    val request: HeartbeatRequest,
)

private class RecordingCoordinatorClient(
    private val routing: ProducerRoutingResponse,
) : CoordinatorClient {
    val requests = CopyOnWriteArrayList<RecordedHeartbeat>()

    override fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse {
        requests += RecordedHeartbeat(streamPrefix, consumerGroup, memberId, request)
        return HeartbeatResponse(
            responseTo = request.requestId,
            status = HeartbeatStatus.OK,
            memberId = memberId,
            memberEpoch = request.memberEpoch + 1,
            heartbeatIntervalMs = 3_000,
            groupEpoch = 1,
            assignmentEpoch = 1,
            metadataVersion = 1,
            assignment = AssignmentView(
                assignedShards = emptySet(),
                pendingShards = emptySet(),
                metadataVersion = 1,
            ),
        )
    }

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        routing.forGroup(streamPrefix, consumerGroup)
}

private class NoopSmartLifecycle : SmartLifecycle {
    override fun start() {
    }

    override fun stop() {
    }

    override fun isRunning(): Boolean = false
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

private fun ProducerRoutingResponse.forGroup(
    streamPrefix: String,
    consumerGroup: String,
): ProducerRoutingResponse =
    if (this.streamPrefix == streamPrefix && this.consumerGroup == consumerGroup) {
        this
    } else {
        routingResponse(streamPrefix = streamPrefix, consumerGroup = consumerGroup, shardCount = shardCount)
    }

private fun Throwable.hasCauseMessage(fragment: String): Boolean =
    generateSequence(this) { it.cause }
        .any { it.message?.contains(fragment) == true }

private fun waitUntil(timeout: Duration = Duration.ofSeconds(5), condition: () -> Boolean): Boolean {
    val deadline = System.nanoTime() + timeout.toNanos()
    while (System.nanoTime() < deadline) {
        if (condition()) {
            return true
        }
        Thread.sleep(10)
    }
    return condition()
}
