package com.redisstream.producer

import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.ProducerRoutingResponse
import com.redisstream.consumer.ProducerRoutingShard
import java.time.Clock
import java.time.Duration
import java.time.Instant

data class ProducerRoute(
    val streamKey: String,
    val shard: ProducerRoutingShard,
    val metadataVersion: Long,
    val activeWriteVersion: Int,
)

class ProducerRoutingCache(
    private val streamPrefix: String,
    private val consumerGroup: String,
    private val client: CoordinatorClient,
    private val refreshInterval: Duration = Duration.ofSeconds(30),
    private val clock: Clock = Clock.systemUTC(),
    private val metrics: RedisStreamProducerMetrics = NoopRedisStreamProducerMetrics,
) {
    private var cached: CachedRouting? = null

    @Synchronized
    fun route(partitionKey: String): ProducerRoute =
        route(partitionKey.toByteArray(Charsets.UTF_8))

    @Synchronized
    fun route(partitionKey: ByteArray): ProducerRoute {
        val routing = currentRouting(forceRefresh = false)
        val shardIndex = RedisStreamPartitionHasher.shardIndex(routing, partitionKey)
        val shard = routing.shards.firstOrNull {
            it.streamVersion == routing.activeWriteVersion && it.shardIndex == shardIndex
        } ?: error(
            "Producer routing metadata version ${routing.metadataVersion} is missing " +
                "active shard ${routing.activeWriteVersion}/$shardIndex",
        )

        return ProducerRoute(
            streamKey = shard.streamKey,
            shard = shard,
            metadataVersion = routing.metadataVersion,
            activeWriteVersion = routing.activeWriteVersion,
        )
    }

    @Synchronized
    fun metadata(): ProducerRoutingResponse =
        currentRouting(forceRefresh = false)

    @Synchronized
    fun refresh(): ProducerRoutingResponse =
        currentRouting(forceRefresh = true)

    @Synchronized
    fun invalidate() {
        invalidate("manual")
    }

    @Synchronized
    internal fun invalidate(reason: String) {
        cached = null
        metrics.recordRoutingCacheInvalidated(reason)
    }

    @Synchronized
    fun cachedMetadataVersion(): Long? =
        cached?.metadata?.metadataVersion

    private fun currentRouting(forceRefresh: Boolean): ProducerRoutingResponse {
        require(streamPrefix.isNotBlank()) { "redis-stream-coordinator.producer.stream-prefix must be set" }
        require(consumerGroup.isNotBlank()) { "redis-stream-coordinator.producer.consumer-group must be set" }

        val now = Instant.now(clock)
        val snapshot = cached
        if (!forceRefresh && snapshot != null && now.isBefore(snapshot.expiresAt)) {
            metrics.recordRoutingCacheHit()
            return snapshot.metadata
        }

        val fetched = try {
            client.producerRouting(streamPrefix, consumerGroup).also(::validate)
        } catch (error: RuntimeException) {
            metrics.recordRoutingRefresh("ERROR", Duration.between(now, Instant.now(clock)))
            throw error
        }
        metrics.recordRoutingRefresh("SUCCESS", Duration.between(now, Instant.now(clock)))
        cached = when {
            snapshot == null -> CachedRouting(fetched, now.plus(refreshInterval))
            snapshot.metadata.metadataVersion == fetched.metadataVersion ->
                CachedRouting(snapshot.metadata, now.plus(refreshInterval))
            else -> CachedRouting(fetched, now.plus(refreshInterval))
        }
        return cached!!.metadata
    }

    private fun validate(metadata: ProducerRoutingResponse) {
        require(metadata.streamPrefix == streamPrefix) {
            "producer routing streamPrefix ${metadata.streamPrefix} does not match configured $streamPrefix"
        }
        require(metadata.consumerGroup == consumerGroup) {
            "producer routing consumerGroup ${metadata.consumerGroup} does not match configured $consumerGroup"
        }
        require(metadata.shardCount > 0) { "producer routing shardCount must be positive" }
        require(metadata.shards.isNotEmpty()) { "producer routing must include active shards" }
        val activeShardIndexes = metadata.shards
            .filter { it.streamVersion == metadata.activeWriteVersion }
            .map { it.shardIndex }
            .toSortedSet()
        require(activeShardIndexes == (0 until metadata.shardCount).toSortedSet()) {
            "producer routing active shard list does not match shardCount"
        }
    }

    private data class CachedRouting(
        val metadata: ProducerRoutingResponse,
        val expiresAt: Instant,
    )
}

object RedisStreamPartitionHasher {
    fun shardIndex(metadata: ProducerRoutingResponse, partitionKey: String): Int =
        shardIndex(metadata, partitionKey.toByteArray(Charsets.UTF_8))

    fun shardIndex(metadata: ProducerRoutingResponse, partitionKey: ByteArray): Int {
        require(metadata.shardCount > 0) { "producer routing shardCount must be positive" }
        val hash = when (metadata.hashAlgorithm.lowercase()) {
            "murmur3", "murmur3_32", "murmur3-32" ->
                Murmur3.hash32(partitionKey, Murmur3.hash32(metadata.hashSeed.toByteArray(Charsets.UTF_8)))
            else -> throw IllegalArgumentException("Unsupported producer routing hash algorithm ${metadata.hashAlgorithm}")
        }
        return ((hash.toLong() and 0xffffffffL) % metadata.shardCount).toInt()
    }
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
