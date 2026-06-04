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
        "spring.data.redis.cluster.nodes=127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103",
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
        val plan = RedisStreamShardProvisioningPlan.forShardCount(
            streamPrefix = uniqueStreamPrefix("direct"),
            consumerGroup = "orders-consumer",
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
        val plan = RedisStreamShardProvisioningPlan.forShardCount(
            streamPrefix = uniqueStreamPrefix("retry"),
            consumerGroup = "orders-consumer",
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
        val initialKeys = RedisStreamShardKeys.forShardCount(streamPrefix, shardCount = 2)
        touch(initialKeys)

        initialKeys.assertConsumerGroup(consumerGroup)

        val migration = coordinator.scaleGroup(
            streamPrefix,
            consumerGroup,
            ScaleGroupRequest(
                targetShardCount = 3,
                requestedBy = "test",
                reason = "integration provisioning",
            ),
        )
        val scaledKeys = RedisStreamShardKeys.forShardCount(streamPrefix, shardCount = 3)
        touch(scaledKeys)

        scaledKeys.assertConsumerGroup(consumerGroup)
    }

    @Test
    fun `monitoring exposes shard offsets lag and paged stream messages`() {
        val streamPrefix = uniqueStreamPrefix("monitoring")
        val consumerGroup = "orders-consumer"

        coordinator.createGroup(streamPrefix, consumerGroup, createGroupRequest(initialShardCount = 1))
        val shardKey = RedisStreamShardKeys.forShard(streamPrefix, shardIndex = 0)
        touch(listOf(shardKey))
        val firstId = redisTemplate.opsForStream<String, String>().add(
            shardKey.value,
            mapOf("payload" to "created", "eventId" to "event-1"),
        )?.value
        val secondId = redisTemplate.opsForStream<String, String>().add(
            shardKey.value,
            mapOf("payload" to "paid", "eventId" to "event-2"),
        )?.value

        val firstHeartbeat = coordinator.heartbeat(
            streamPrefix,
            consumerGroup,
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        coordinator.heartbeat(
            streamPrefix,
            consumerGroup,
            "member-a",
            heartbeat(
                memberId = "member-a",
                memberEpoch = firstHeartbeat.memberEpoch,
                ownedShards = firstHeartbeat.assignment.assignedShards,
                shardProgress = listOf(
                    ShardConsumptionProgress(
                        shard = ShardId(0),
                        streamKey = shardKey.value,
                        lastDeliveredId = secondId,
                        lastAckedId = firstId,
                        pendingCount = 1,
                    ),
                ),
            ),
        )

        val offsets = coordinator.streamShardOffsets(streamPrefix, consumerGroup)
        val messages = coordinator.streamMessages(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            shardIndex = 0,
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = null,
            limit = 1,
        )

        assertEquals(1, offsets.shards.size)
        assertEquals(shardKey.value, offsets.shards.single().streamKey)
        assertEquals(2, offsets.shards.single().streamLength)
        assertEquals(firstId, offsets.shards.single().consumerLastAckedId)
        assertEquals(1, messages.records.size)
        assertEquals(secondId, messages.records.single().recordId)
        assertEquals("paid", messages.records.single().payload)
        assertEquals(secondId, messages.nextCursor)
    }

    private fun createGroupRequest(initialShardCount: Int): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun uniqueStreamPrefix(label: String): String =
        "redis-it-stream-$label-${UUID.randomUUID()}"

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
        shardProgress: List<ShardConsumptionProgress> = emptyList(),
    ): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
            ownedShards = ownedShards,
            shardProgress = shardProgress,
        )

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
