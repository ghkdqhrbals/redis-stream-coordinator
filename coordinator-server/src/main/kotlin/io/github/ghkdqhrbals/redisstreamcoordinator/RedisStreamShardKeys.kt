package io.github.ghkdqhrbals.redisstreamcoordinator

import java.nio.charset.StandardCharsets

data class RedisStreamShardKey(
    val streamPrefix: String,
    val streamVersion: Int,
    val shardIndex: Int,
) {
    init {
        require(streamPrefix.isNotBlank()) { "streamPrefix must not be blank" }
        require('{' !in streamPrefix && '}' !in streamPrefix) {
            "streamPrefix must not contain Redis Cluster hash tag braces"
        }
        require(streamVersion > 0) { "streamVersion must be positive" }
        require(shardIndex >= 0) { "shardIndex must be zero or positive" }
    }

    val value: String = "$streamPrefix:v$streamVersion:shard:$shardIndex"
    val slot: Int = RedisClusterHashSlot.slot(value)
}

object RedisStreamShardKeys {
    fun forShard(streamPrefix: String, streamVersion: Int, shardIndex: Int): RedisStreamShardKey =
        RedisStreamShardKey(streamPrefix, streamVersion, shardIndex)

    fun forVersion(streamPrefix: String, streamVersion: Int, shardCount: Int): List<RedisStreamShardKey> {
        require(shardCount > 0) { "shardCount must be positive" }
        return (0 until shardCount).map { shardIndex ->
            forShard(streamPrefix, streamVersion, shardIndex)
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

fun GroupMetadata.streamShardKeys(streamVersion: Int = activeWriteVersion): List<RedisStreamShardKey> {
    val shardCount = shardCountsByVersion[streamVersion]
        ?: throw IllegalArgumentException("Unknown stream version $streamVersion")
    return RedisStreamShardKeys.forVersion(streamPrefix, streamVersion, shardCount)
}
