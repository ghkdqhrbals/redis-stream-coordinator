package io.github.ghkdqhrbals.redisstreamcoordinator

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.redis.core.StringRedisTemplate
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.readValue
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@EnabledIfEnvironmentVariable(named = "REDIS_COORDINATOR_INTEGRATION_TESTS", matches = "true")
@SpringBootTest(
    properties = [
        "coordinator.store.type=redis",
        "coordinator.store.key-prefix=redis-stream:coord:test",
        "coordinator.defaults.initial-shard-count=2",
        "coordinator.defaults.consumer-max-concurrency=4",
    ],
)
class RedisCoordinatorStateStoreIntegrationTest {
    @Autowired
    private lateinit var coordinator: CoordinatorService

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private val stateKeys = RedisCoordinatorStateKeys("redis-stream:coord:test")
    private val touchedGroups = mutableListOf<GroupKey>()

    @AfterEach
    fun cleanup() {
        touchedGroups.forEach { key ->
            val keys = stateKeys.forGroup(key)
            redisTemplate.delete(
                listOf(
                    keys.group,
                    keys.members,
                    keys.targetAssignments,
                    keys.currentAssignments,
                    keys.migrations,
                    keys.activeMigration,
                ),
            )
            redisTemplate.opsForSet().remove(stateKeys.groupsIndex, keys.group)
        }
    }

    @Test
    fun `redis store writes aggregate and projected PRD keys`() {
        val key = GroupKey("redis-it-orders", "orders-consumer")
        touchedGroups += key

        coordinator.createGroup(key.streamPrefix, key.consumerGroup, createGroupRequest(initialShardCount = 2))
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

        val keys = stateKeys.forGroup(key)
        val storedGroup = redisTemplate.opsForValue().get(keys.group)
            ?.let { objectMapper.readValue<GroupMetadata>(it) }
        val storedMember = redisTemplate.opsForHash<String, String>().get(keys.members, "member-a")
            ?.let { objectMapper.readValue<MemberMetadata>(it) }
        val targetAssignment = redisTemplate.opsForHash<String, String>().get(keys.targetAssignments, "member-a")
            ?.let { objectMapper.readValue<Set<ShardId>>(it) }
        val currentAssignment = redisTemplate.opsForHash<String, String>().get(keys.currentAssignments, "member-a")
            ?.let { objectMapper.readValue<Set<ShardId>>(it) }
        val storedMigration = redisTemplate.opsForHash<String, String>().get(keys.migrations, migration.migrationId)
            ?.let { objectMapper.readValue<Migration>(it) }

        assertNotNull(storedGroup)
        assertEquals(2, storedGroup.activeWriteVersion)
        assertEquals("member-a", assertNotNull(storedMember).memberId)
        assertTrue(assertNotNull(targetAssignment).containsAll(setOf(ShardId(1, 0), ShardId(1, 1))))
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), currentAssignment)
        assertEquals(MigrationState.ACTIVE, assertNotNull(storedMigration).state)
        assertEquals(migration.migrationId, redisTemplate.opsForValue().get(keys.activeMigration))
    }

    private fun createGroupRequest(initialShardCount: Int): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            hashAlgorithm = "murmur3",
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
            rebalanceTimeoutMs = 60_000,
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
            ownedShards = ownedShards,
        )
}
