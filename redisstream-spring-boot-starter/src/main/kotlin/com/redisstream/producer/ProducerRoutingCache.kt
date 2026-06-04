package com.redisstream.producer

import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.CoordinatorRoutingMetadataValidator
import com.redisstream.consumer.ProducerRoutingResponse
import com.redisstream.consumer.ProducerRoutingShard
import java.time.Clock
import java.time.Duration
import java.time.Instant

data class ProducerRoute(
    val streamKey: String,
    val shard: ProducerRoutingShard,
    val metadataVersion: Long,
)

class ProducerRoutingCache(
    private val streamPrefix: String,
    private val consumerGroupName: String,
    private val client: CoordinatorClient,
    private val refreshInterval: Duration = Duration.ofSeconds(30),
    private val clock: Clock = Clock.systemUTC(),
) {
    private var cached: CachedRouting? = null

    /**
     * Routes a UTF-8 partition key to the active Redis Stream shard.
     */
    @Synchronized
    fun route(partitionKey: String): ProducerRoute =
        route(partitionKey.toByteArray(Charsets.UTF_8))

    /**
     * Routes a binary partition key using the coordinator-provided active shard metadata.
     */
    @Synchronized
    fun route(partitionKey: ByteArray): ProducerRoute {
        val routing = currentRouting(forceRefresh = false)
        val shardIndex = RedisStreamPartitionHasher.shardIndex(routing, partitionKey)
        val shard = routing.shards.firstOrNull { it.shardIndex == shardIndex } ?: error(
            "Producer routing metadata version ${routing.metadataVersion} is missing " +
                "shard $shardIndex",
        )

        return ProducerRoute(
            streamKey = shard.streamKey,
            shard = shard,
            metadataVersion = routing.metadataVersion,
        )
    }

    /**
     * Returns the cached routing metadata, refreshing it when the cache has expired.
     */
    @Synchronized
    fun metadata(): ProducerRoutingResponse =
        currentRouting(forceRefresh = false)

    /**
     * Forces a metadata fetch from the coordinator and updates the local routing cache.
     */
    @Synchronized
    fun refresh(): ProducerRoutingResponse =
        currentRouting(forceRefresh = true)

    /**
     * Clears routing metadata so the next publish sees the latest coordinator assignment.
     */
    @Synchronized
    fun invalidate() {
        invalidate("manual")
    }

    @Synchronized
    internal fun invalidate(reason: String) {
        cached = null
    }

    @Synchronized
    fun cachedMetadataVersion(): Long? =
        cached?.metadata?.metadataVersion

    /**
     * Loads and validates initial coordinator routing metadata so missing shards fail application startup.
     */
    @Synchronized
    fun validateInitialRouting(): ProducerRoutingResponse =
        currentRouting(forceRefresh = true)

    /**
     * Loads routing metadata while preserving the existing cache when the version is unchanged.
     */
    private fun currentRouting(forceRefresh: Boolean): ProducerRoutingResponse {
        require(streamPrefix.isNotBlank()) { "ProducerRoutingProperties.streamPrefix must be set" }
        require(consumerGroupName.isNotBlank()) { "ProducerRoutingProperties.consumerGroupName must be set" }
        require(!refreshInterval.isNegative && !refreshInterval.isZero) {
            "ProducerRoutingProperties.routingRefreshInterval must be positive"
        }

        val now = Instant.now(clock)
        val snapshot = cached
        if (!forceRefresh && snapshot != null && now.isBefore(snapshot.expiresAt)) {
            return snapshot.metadata
        }

        val fetched = client.producerRouting(streamPrefix, consumerGroupName).also {
            CoordinatorRoutingMetadataValidator.validate(streamPrefix, consumerGroupName, it)
        }
        cached = when {
            snapshot == null -> CachedRouting(fetched, now.plus(refreshInterval))
            snapshot.metadata.metadataVersion == fetched.metadataVersion ->
                CachedRouting(snapshot.metadata, now.plus(refreshInterval))
            else -> CachedRouting(fetched, now.plus(refreshInterval))
        }
        return cached!!.metadata
    }

    private data class CachedRouting(
        val metadata: ProducerRoutingResponse,
        val expiresAt: Instant,
    )
}

object RedisStreamPartitionHasher {
    private const val HASH_SPACE_SIZE = 4_294_967_296L

    /**
     * Maps a UTF-8 partition key to a shard index with the stable v1 routing algorithm.
     */
    fun shardIndex(metadata: ProducerRoutingResponse, partitionKey: String): Int =
        shardIndex(metadata, partitionKey.toByteArray(Charsets.UTF_8))

    /**
     * Maps a binary partition key to a shard index without modulo bias.
     */
    fun shardIndex(metadata: ProducerRoutingResponse, partitionKey: ByteArray): Int {
        require(metadata.shardCount > 0) { "producer routing shardCount must be positive" }
        return unbiasedShardIndex(partitionKey, metadata.shardCount)
    }

    /**
     * Rehashes only the rare out-of-range values so every shard receives an equal-sized hash range.
     */
    private fun unbiasedShardIndex(partitionKey: ByteArray, shardCount: Int): Int {
        val limit = HASH_SPACE_SIZE - (HASH_SPACE_SIZE % shardCount.toLong())
        var attempt = 0
        while (true) {
            val hash = if (attempt == 0) {
                unsignedHash(partitionKey, 0)
            } else {
                unsignedHash(partitionKey, Murmur3.hash32(attempt.toLittleEndianBytes()))
            }
            if (hash < limit) {
                return (hash % shardCount).toInt()
            }
            attempt++
        }
    }

    private fun unsignedHash(partitionKey: ByteArray, seed: Int): Long =
        Murmur3.hash32(partitionKey, seed).toLong() and 0xffffffffL

    private fun Int.toLittleEndianBytes(): ByteArray =
        byteArrayOf(
            (this and 0xff).toByte(),
            ((this ushr 8) and 0xff).toByte(),
            ((this ushr 16) and 0xff).toByte(),
            ((this ushr 24) and 0xff).toByte(),
        )
}

private object Murmur3 {
    fun hash32(data: ByteArray, seed: Int = 0): Int {
        var hash = seed
        val roundedEnd = data.size and -4

        var index = 0
        while (index < roundedEnd) {
            var k = (data[index].toInt() and 0xff) or
                ((data[index + 1].toInt() and 0xff) shl 8) or
                ((data[index + 2].toInt() and 0xff) shl 16) or
                (data[index + 3].toInt() shl 24)
            k *= C1
            k = Integer.rotateLeft(k, 15)
            k *= C2

            hash = hash xor k
            hash = Integer.rotateLeft(hash, 13)
            hash = hash * 5 + 0xe6546b64.toInt()
            index += 4
        }

        var k1 = 0
        when (data.size and 3) {
            3 -> {
                k1 = k1 xor ((data[roundedEnd + 2].toInt() and 0xff) shl 16)
                k1 = k1 xor ((data[roundedEnd + 1].toInt() and 0xff) shl 8)
                k1 = k1 xor (data[roundedEnd].toInt() and 0xff)
            }
            2 -> {
                k1 = k1 xor ((data[roundedEnd + 1].toInt() and 0xff) shl 8)
                k1 = k1 xor (data[roundedEnd].toInt() and 0xff)
            }
            1 -> k1 = k1 xor (data[roundedEnd].toInt() and 0xff)
        }

        if (k1 != 0) {
            k1 *= C1
            k1 = Integer.rotateLeft(k1, 15)
            k1 *= C2
            hash = hash xor k1
        }

        hash = hash xor data.size
        hash = hash xor (hash ushr 16)
        hash *= 0x85ebca6b.toInt()
        hash = hash xor (hash ushr 13)
        hash *= 0xc2b2ae35.toInt()
        hash = hash xor (hash ushr 16)
        return hash
    }

    private const val C1 = -0x3361d2af
    private const val C2 = 0x1b873593
}
