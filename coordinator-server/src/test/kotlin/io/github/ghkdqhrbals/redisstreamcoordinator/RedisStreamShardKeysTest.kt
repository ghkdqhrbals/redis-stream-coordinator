package io.github.ghkdqhrbals.redisstreamcoordinator

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
        val keys = RedisStreamShardKeys.forVersion("orders", streamVersion = 1, shardCount = 120)

        assertEquals("orders:v1:shard:0", keys.first().value)
        assertTrue(keys.none { "{" in it.value || "}" in it.value })

        val distribution = RedisStreamShardKeys.distributionForEqualMasterRanges(keys, masterCount = 3)
        val nonEmptyMasters = distribution.filterValues { it > 0 }

        assertEquals(setOf(0, 1, 2), nonEmptyMasters.keys)
        assertTrue(distribution.values.max() - distribution.values.min() <= 5)
    }

    @Test
    fun `stream shard key helper validates Redis Cluster unsafe metadata`() {
        assertFailsWith<IllegalArgumentException> {
            RedisStreamShardKeys.forVersion("orders:{tenant-a}", streamVersion = 1, shardCount = 4)
        }
        assertFailsWith<IllegalArgumentException> {
            RedisStreamShardKeys.forVersion("orders", streamVersion = 0, shardCount = 4)
        }
        assertFailsWith<IllegalArgumentException> {
            RedisStreamShardKeys.forVersion("orders", streamVersion = 1, shardCount = 0)
        }
    }

    @Test
    fun `group metadata creates stream shard keys for known versions only`() {
        val group = GroupMetadata(
            streamPrefix = "payments",
            consumerGroup = "payments-consumer",
            groupEpoch = 1,
            metadataVersion = 1,
            assignmentEpoch = 0,
            state = GroupState.EMPTY,
            activeWriteVersion = 2,
            readableVersions = setOf(1, 2),
            shardCountsByVersion = linkedMapOf(1 to 2, 2 to 4),
            hashAlgorithm = "murmur3",
            hashSeed = "default",
            consumerConcurrencyPolicy = ConsumerConcurrencyPolicy(defaultMaxConcurrency = 4),
            createdAt = Instant.parse("2026-05-22T00:00:00Z"),
            updatedAt = Instant.parse("2026-05-22T00:00:00Z"),
        )

        val keys = group.streamShardKeys()

        assertEquals(
            listOf(
                "payments:v2:shard:0",
                "payments:v2:shard:1",
                "payments:v2:shard:2",
                "payments:v2:shard:3",
            ),
            keys.map { it.value },
        )
        assertFailsWith<IllegalArgumentException> {
            group.streamShardKeys(streamVersion = 3)
        }
    }
}
