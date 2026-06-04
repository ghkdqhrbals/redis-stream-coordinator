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
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.readValue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import java.nio.charset.StandardCharsets
import java.time.Instant

@EnabledIfEnvironmentVariable(named = "REDIS_COORDINATOR_INTEGRATION_TESTS", matches = "true")
@SpringBootTest(
    properties = [
        "coordinator.store.type=redis",
        "coordinator.store.key-prefix=redis-stream:coord:test",
        "coordinator.streams.provisioning-enabled=true",
        "coordinator.defaults.initial-shard-count=2",
        "coordinator.defaults.consumer-max-concurrency=4",
        "spring.data.redis.cluster.nodes=127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103",
    ],
)
class RedisCoordinatorStateStoreIntegrationTest {
    @Autowired
    private lateinit var coordinator: CoordinatorService

    @Autowired
    private lateinit var stateStore: CoordinatorStateStore

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private val stateKeys = RedisCoordinatorStateKeys("redis-stream:coord:test")
    private val touchedGroups = mutableListOf<GroupKey>()
    private val touchedStreamKeys = mutableSetOf<String>()

    @AfterEach
    fun cleanup() {
        touchedGroups.forEach { key ->
            val keys = stateKeys.forGroup(key)
            redisTemplate.delete(keys.metadata)
            redisTemplate.opsForSet().remove(stateKeys.groupsIndex, keys.metadata)
        }
        touchedStreamKeys.forEach { redisTemplate.delete(it) }
    }

    @Test
    fun `redis store writes one metadata hash key`() {
        val key = GroupKey("redis-it-orders", "orders-consumer")
        touchedGroups += key

        coordinator.createGroup(key.streamPrefix, key.consumerGroup, createGroupRequest(initialShardCount = 2))
        touchStreamKeys(key.streamPrefix, shardCount = 2)
        val first = coordinator.heartbeat(
            key.streamPrefix,
            key.consumerGroup,
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        coordinator.heartbeat(
            key.streamPrefix,
            key.consumerGroup,
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        val migration = coordinator.scaleGroup(
            key.streamPrefix,
            key.consumerGroup,
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "redis projection"),
        )
        touchStreamKeys(key.streamPrefix, shardCount = 3)

        val keys = stateKeys.forGroup(key)
        val storedGroup = redisTemplate.opsForHash<String, String>().get(keys.metadata, "aggregate")
            ?.let { objectMapper.readValue<GroupMetadata>(it) }
        val revision = redisTemplate.opsForHash<String, String>().get(keys.metadata, "revision")

        assertNotNull(storedGroup)
                assertEquals("member-a", storedGroup.members.getValue("member-a").memberId)
        assertTrue(storedGroup.targetAssignments.getValue("member-a").containsAll(setOf(ShardId(0), ShardId(1))))
        assertEquals(setOf(ShardId(0), ShardId(1)), storedGroup.members.getValue("member-a").currentAssignment)
        assertEquals(MigrationState.ACTIVE, storedGroup.migrations.getValue(migration.reshardingId).state)
        assertEquals(migration.reshardingId, storedGroup.activeReshardingId)
        assertEquals(storedGroup.storeRevision.toString(), revision)
    }

    @Test
    fun `redis store rejects stale snapshot writes`() {
        val key = GroupKey("redis-it-conflict", "orders-consumer")
        touchedGroups += key

        coordinator.createGroup(key.streamPrefix, key.consumerGroup, createGroupRequest(initialShardCount = 2))
        touchStreamKeys(key.streamPrefix, shardCount = 2)
        val firstSnapshot = assertNotNull(stateStore.get(key))
        val staleSnapshot = assertNotNull(stateStore.get(key))

        firstSnapshot.members["member-a"] = member("member-a")
        firstSnapshot.targetAssignments["member-a"] = mutableSetOf(ShardId(0))
        firstSnapshot.metadataVersion += 1
        stateStore.save(key, firstSnapshot)

        staleSnapshot.members["member-b"] = member("member-b")
        staleSnapshot.targetAssignments["member-b"] = mutableSetOf(ShardId(1))
        staleSnapshot.metadataVersion += 1

        assertFailsWith<CoordinatorStateConflictException> {
            stateStore.save(key, staleSnapshot)
        }

        val stored = assertNotNull(stateStore.get(key))
        assertEquals(setOf("member-a"), stored.members.keys)
        assertEquals(setOf("member-a"), stored.targetAssignments.keys)
        assertEquals(firstSnapshot.storeRevision, stored.storeRevision)
        assertTrue(firstSnapshot.storeRevision > staleSnapshot.storeRevision)
    }

    @Test
    fun `redis stream provisioning creates consumer groups for created and scaled shard versions`() {
        val key = GroupKey("redis-it-streams", "orders-consumer")
        touchedGroups += key

        coordinator.createGroup(key.streamPrefix, key.consumerGroup, createGroupRequest(initialShardCount = 2))
        val initialKeys = RedisStreamShardKeys.forShardCount(key.streamPrefix, shardCount = 2)
        touchStreamKeys(initialKeys)

        initialKeys.assertConsumerGroups(key.consumerGroup)

        val migration = coordinator.scaleGroup(
            key.streamPrefix,
            key.consumerGroup,
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "provision streams"),
        )
        val scaledKeys = RedisStreamShardKeys.forShardCount(
            key.streamPrefix,
            shardCount = 3,
        )
        touchStreamKeys(scaledKeys)

        scaledKeys.assertConsumerGroups(key.consumerGroup)
    }

    private fun createGroupRequest(initialShardCount: Int): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
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
        )

    private fun member(memberId: String): MemberMetadata =
        MemberMetadata(
            memberId = memberId,
            memberName = memberId,
            state = MemberState.ACTIVE,
            memberEpoch = 1,
            metadataVersion = 1,
            assignedMaxConcurrency = 4,
            runtimeMaxConcurrency = 4,
            activeConsumerWorkers = 0,
            currentAssignment = emptySet(),
            revoking = emptySet(),
            lastHeartbeatAt = Instant.parse("2026-05-21T00:00:00Z"),
            memberLeaseExpiresAt = Instant.parse("2026-05-21T00:00:15Z"),
        )

    private fun touchStreamKeys(streamPrefix: String, shardCount: Int) {
        touchStreamKeys(RedisStreamShardKeys.forShardCount(streamPrefix, shardCount))
    }

    private fun touchStreamKeys(shardKeys: List<RedisStreamShardKey>) {
        touchedStreamKeys += shardKeys.map { it.value }
    }

    private fun List<RedisStreamShardKey>.assertConsumerGroups(consumerGroup: String) {
        forEach { shardKey ->
            val groupNames = redisTemplate.execute { connection ->
                connection.streamCommands().xInfoGroups(shardKey.value.toByteArray(StandardCharsets.UTF_8))
                    .map { it.groupName() }
                    .toList()
            }.orEmpty()

            assertTrue(
                consumerGroup in groupNames,
                "Expected Redis Stream ${shardKey.value} to contain consumer group $consumerGroup",
            )
        }
    }
}
