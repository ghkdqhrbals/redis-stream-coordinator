package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import java.nio.charset.StandardCharsets
import java.util.UUID

@EnabledIfEnvironmentVariable(named = "REDIS_COORDINATOR_INTEGRATION_TESTS", matches = "true")
@SpringBootTest(
    properties = [
        "coordinator.store.type=memory",
        "coordinator.streams.provisioning-enabled=true",
        "coordinator.defaults.initial-shard-count=2",
        "coordinator.defaults.consumer-max-concurrency=4",
    ],
)
class RedisStreamProvisioningIntegrationTest {
    @Autowired
    private lateinit var coordinator: CoordinatorService

    @Autowired
    private lateinit var streamProvisioner: StreamShardProvisioner

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    private val touchedStreamKeys = mutableSetOf<String>()

    @AfterEach
    fun cleanup() {
        touchedStreamKeys.forEach { redisTemplate.delete(it) }
    }

    @Test
    fun `stream provisioner is idempotent for existing Redis consumer groups`() {
        val plan = RedisStreamShardProvisioningPlan.forVersion(
            streamPrefix = uniqueStreamPrefix("direct"),
            consumerGroup = "orders-consumer",
            streamVersion = 1,
            shardCount = 2,
        )
        touch(plan)

        streamProvisioner.provision(plan)
        streamProvisioner.provision(plan)

        plan.shardKeys.forEach { shardKey ->
            assertEquals(listOf(plan.consumerGroup), consumerGroupNames(shardKey))
        }
    }

    @Test
    fun `stream provisioner retry succeeds after partial Redis failure leaves existing groups`() {
        val plan = RedisStreamShardProvisioningPlan.forVersion(
            streamPrefix = uniqueStreamPrefix("retry"),
            consumerGroup = "orders-consumer",
            streamVersion = 1,
            shardCount = 3,
        )
        val poisonedShardKey = plan.shardKeys[1]
        touch(plan)
        redisTemplate.opsForValue().set(poisonedShardKey.value, "not-a-stream")

        val error = assertFailsWith<CoordinatorException> {
            streamProvisioner.provision(plan)
        }

        assertEquals(CoordinatorError.REDIS_STREAM_PROVISIONING_FAILED, error.error)
        assertEquals(listOf(plan.consumerGroup), consumerGroupNames(plan.shardKeys.first()))

        redisTemplate.delete(poisonedShardKey.value)
        streamProvisioner.provision(plan)

        plan.shardKeys.forEach { shardKey ->
            assertEquals(listOf(plan.consumerGroup), consumerGroupNames(shardKey))
        }
    }

    @Test
    fun `coordinator provisions Redis consumer groups on create and scale`() {
        val streamPrefix = uniqueStreamPrefix("coordinator")
        val consumerGroup = "orders-consumer"

        coordinator.createGroup(streamPrefix, consumerGroup, createGroupRequest(initialShardCount = 2))
        val version1Keys = RedisStreamShardKeys.forVersion(streamPrefix, streamVersion = 1, shardCount = 2)
        touch(version1Keys)

        version1Keys.assertConsumerGroup(consumerGroup)

        val migration = coordinator.scaleGroup(
            streamPrefix,
            consumerGroup,
            ScaleGroupRequest(
                targetShardCount = 3,
                requestedBy = "test",
                reason = "integration provisioning",
            ),
        )
        val version2Keys = RedisStreamShardKeys.forVersion(streamPrefix, migration.toVersion, shardCount = 3)
        touch(version2Keys)

        assertEquals(2, migration.toVersion)
        version2Keys.assertConsumerGroup(consumerGroup)
    }

    private fun createGroupRequest(initialShardCount: Int): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            hashAlgorithm = "murmur3",
            requestedBy = "test",
        )

    private fun uniqueStreamPrefix(label: String): String =
        "redis-it-stream-$label-${UUID.randomUUID()}"

    private fun touch(plan: RedisStreamShardProvisioningPlan) {
        touch(plan.shardKeys)
    }

    private fun touch(shardKeys: List<RedisStreamShardKey>) {
        touchedStreamKeys += shardKeys.map { it.value }
    }

    private fun List<RedisStreamShardKey>.assertConsumerGroup(consumerGroup: String) {
        forEach { shardKey ->
            assertTrue(
                consumerGroup in consumerGroupNames(shardKey),
                "Expected Redis Stream ${shardKey.value} to contain consumer group $consumerGroup",
            )
        }
    }

    private fun consumerGroupNames(shardKey: RedisStreamShardKey): List<String> =
        redisTemplate.execute { connection ->
            connection.streamCommands().xInfoGroups(shardKey.value.toByteArray(StandardCharsets.UTF_8))
                .map { it.groupName() }
                .toList()
        }.orEmpty()
}
