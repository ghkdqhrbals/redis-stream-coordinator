package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import java.time.Instant

class RedisStreamShardKeysTest {
    @Test
    fun `hash slot matches Redis Cluster hash tag behavior`() {
        assertEquals(12_739, RedisClusterHashSlot.slot("123456789"))
        assertEquals(
            RedisClusterHashSlot.slot("{user1000}.following"),
            RedisClusterHashSlot.slot("{user1000}.followers"),
        )
    }

    @Test
    fun `stream shard keys avoid hash tags and distribute across equal master ranges`() {
        val keys = RedisStreamShardKeys.forShardCount("orders", shardCount = 120)

        assertEquals("orders:0", keys.first().value)
        assertTrue(keys.none { "{" in it.value || "}" in it.value })

        val distribution = RedisStreamShardKeys.distributionForEqualMasterRanges(keys, masterCount = 3)
        val nonEmptyMasters = distribution.filterValues { it > 0 }

        assertEquals(setOf(0, 1, 2), nonEmptyMasters.keys)
        assertTrue(distribution.values.max() - distribution.values.min() <= 8)
    }

    @Test
    fun `stream key pattern uses placeholders for producer routing`() {
        assertEquals("orders:{shardIndex}", RedisStreamShardKeys.keyPattern("orders"))
    }

    @Test
    fun `stream shard key helper validates Redis Cluster unsafe metadata`() {
        assertFailsWith<IllegalArgumentException> {
            RedisStreamShardKeys.forShardCount("orders:{tenant-a}", shardCount = 4)
        }
        assertFailsWith<IllegalArgumentException> {
            RedisStreamShardKeys.forShardCount("orders", shardCount = 0)
        }
    }

    @Test
    fun `group metadata creates stream shard keys for configured shard count`() {
        val group = GroupMetadata(
            streamPrefix = "payments",
            consumerGroup = "payments-consumer",
            groupEpoch = 1,
            metadataVersion = 1,
            assignmentEpoch = 0,
            state = GroupState.EMPTY,
            shardCount = 4,
            consumerConcurrencyPolicy = ConsumerConcurrencyPolicy(defaultMaxConcurrency = 4),
            createdAt = Instant.parse("2026-05-22T00:00:00Z"),
            updatedAt = Instant.parse("2026-05-22T00:00:00Z"),
        )

        val keys = group.streamShardKeys()

        assertEquals(
            listOf(
                "payments:0",
                "payments:1",
                "payments:2",
                "payments:3",
            ),
            keys.map { it.value },
        )
    }

    @Test
    fun `provisioning plan binds consumer group and shard keys`() {
        val plan = RedisStreamShardProvisioningPlan.forShardCount(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            shardCount = 3,
        )

        assertEquals("orders", plan.streamPrefix)
        assertEquals("orders-consumer", plan.consumerGroup)
        assertEquals(3, plan.shardCount)
        assertEquals(
            listOf("orders:0", "orders:1", "orders:2"),
            plan.shardKeys.map { it.value },
        )
    }
}
