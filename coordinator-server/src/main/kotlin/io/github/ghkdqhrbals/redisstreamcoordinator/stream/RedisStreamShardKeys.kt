package io.github.ghkdqhrbals.redisstreamcoordinator.stream

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import java.nio.charset.StandardCharsets

data class RedisStreamShardKey(
    val streamPrefix: String,
    val shardIndex: Int,
) {
    init {
        validateStreamPrefix(streamPrefix)
        require(shardIndex >= 0) { "shardIndex must be zero or positive" }
    }

    val value: String = "$streamPrefix:$shardIndex"
    val slot: Int = RedisClusterHashSlot.slot(value)
}

data class RedisStreamShardProvisioningPlan(
    val streamPrefix: String,
    val consumerGroup: String,
    val shardCount: Int,
    val shardKeys: List<RedisStreamShardKey>,
) {
    init {
        require(consumerGroup.isNotBlank()) { "consumerGroup must not be blank" }
        require(shardCount > 0) { "shardCount must be positive" }
        require(shardKeys.size == shardCount) { "shardKeys size must match shardCount" }
    }

    companion object {
        fun forShardCount(
            streamPrefix: String,
            consumerGroup: String,
            shardCount: Int,
        ): RedisStreamShardProvisioningPlan =
            RedisStreamShardProvisioningPlan(
                streamPrefix = streamPrefix,
                consumerGroup = consumerGroup,
                shardCount = shardCount,
                shardKeys = RedisStreamShardKeys.forShardCount(streamPrefix, shardCount),
            )
    }
}

object RedisStreamShardKeys {
    fun keyPattern(streamPrefix: String): String {
        validateStreamPrefix(streamPrefix)
        return "$streamPrefix:{shardIndex}"
    }

    fun forShard(streamPrefix: String, shardIndex: Int): RedisStreamShardKey =
        RedisStreamShardKey(streamPrefix, shardIndex)

    fun forShardCount(streamPrefix: String, shardCount: Int): List<RedisStreamShardKey> {
        require(shardCount > 0) { "shardCount must be positive" }
        return (0 until shardCount).map { shardIndex ->
            forShard(streamPrefix, shardIndex)
        }
    }

    fun distributionForEqualMasterRanges(
        keys: Iterable<RedisStreamShardKey>,
        masterCount: Int,
    ): Map<Int, Int> {
        require(masterCount > 0) { "masterCount must be positive" }
        val counts = (0 until masterCount).associateWith { 0 }.toMutableMap()
        keys.forEach { key ->
            val masterIndex = RedisClusterHashSlot.equalMasterRangeIndex(key.slot, masterCount)
            counts[masterIndex] = counts.getValue(masterIndex) + 1
        }
        return counts.toSortedMap()
    }
}

private fun validateStreamPrefix(streamPrefix: String) {
    require(streamPrefix.isNotBlank()) { "streamPrefix must not be blank" }
    require('{' !in streamPrefix && '}' !in streamPrefix) {
        "streamPrefix must not contain Redis Cluster hash tag braces"
    }
}

object RedisClusterHashSlot {
    const val SLOT_COUNT: Int = 16_384

    fun slot(key: String): Int =
        crc16(hashInput(key).toByteArray(StandardCharsets.UTF_8)) and (SLOT_COUNT - 1)

    fun equalMasterRangeIndex(slot: Int, masterCount: Int): Int {
        require(slot in 0 until SLOT_COUNT) { "slot must be between 0 and ${SLOT_COUNT - 1}" }
        require(masterCount > 0) { "masterCount must be positive" }
        return ((slot.toLong() * masterCount) / SLOT_COUNT).toInt().coerceAtMost(masterCount - 1)
    }

    private fun hashInput(key: String): String {
        val tagStart = key.indexOf('{')
        if (tagStart < 0) return key

        val tagEnd = key.indexOf('}', tagStart + 1)
        if (tagEnd < 0 || tagEnd == tagStart + 1) return key

        return key.substring(tagStart + 1, tagEnd)
    }

    private fun crc16(bytes: ByteArray): Int {
        var crc = 0
        bytes.forEach { byte ->
            crc = crc xor ((byte.toInt() and 0xff) shl 8)
            repeat(8) {
                crc = if ((crc and 0x8000) != 0) {
                    (crc shl 1) xor 0x1021
                } else {
                    crc shl 1
                }
                crc = crc and 0xffff
            }
        }
        return crc
    }
}

fun GroupMetadata.streamShardKeys(): List<RedisStreamShardKey> =
    RedisStreamShardKeys.forShardCount(streamPrefix, shardCount)

fun GroupMetadata.streamShardProvisioningPlan(): RedisStreamShardProvisioningPlan =
    RedisStreamShardProvisioningPlan.forShardCount(streamPrefix, consumerGroup, shardCount)
