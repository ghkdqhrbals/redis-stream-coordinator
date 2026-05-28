package com.redisstream.producer

import com.redisstream.consumer.AssignmentView
import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.HeartbeatRequest
import com.redisstream.consumer.HeartbeatResponse
import com.redisstream.consumer.ProducerRoutingResponse
import com.redisstream.consumer.ProducerRoutingShard
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class ProducerRoutingCacheTest {
    @Test
    fun `route reuses cached producer routing metadata before refresh interval expires`() {
        val client = ScriptedRoutingClient(routing(version = 1, activeWriteVersion = 1, shardCount = 2))
        val clock = MutableClock(Instant.parse("2026-05-23T00:00:00Z"))
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            client = client,
            refreshInterval = Duration.ofSeconds(30),
            clock = clock,
        )

        val first = cache.route("order-1")
        clock.advance(Duration.ofSeconds(5))
        val second = cache.route("order-2")

        assertEquals(1, client.calls)
        assertEquals(1, first.activeWriteVersion)
        assertEquals(1, second.activeWriteVersion)
        assertEquals(1, cache.cachedMetadataVersion())
    }

    @Test
    fun `route refreshes expired cache and replaces metadata when metadata version changes`() {
        val client = ScriptedRoutingClient(
            routing(version = 1, activeWriteVersion = 1, shardCount = 2),
            routing(version = 2, activeWriteVersion = 2, shardCount = 4),
        )
        val clock = MutableClock(Instant.parse("2026-05-23T00:00:00Z"))
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            client = client,
            refreshInterval = Duration.ofSeconds(10),
            clock = clock,
        )

        val before = cache.route("order-1")
        clock.advance(Duration.ofSeconds(11))
        val after = cache.route("order-1")

        assertEquals(2, client.calls)
        assertEquals(1, before.activeWriteVersion)
        assertEquals(2, after.activeWriteVersion)
        assertEquals(2, after.metadataVersion)
        assertEquals(2, cache.cachedMetadataVersion())
    }

    @Test
    fun `forced refresh checks coordinator even before refresh interval expires`() {
        val client = ScriptedRoutingClient(
            routing(version = 1, activeWriteVersion = 1, shardCount = 2),
            routing(version = 2, activeWriteVersion = 2, shardCount = 3),
        )
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            client = client,
            refreshInterval = Duration.ofMinutes(5),
        )

        cache.route("order-1")
        val refreshed = cache.refresh()

        assertEquals(2, client.calls)
        assertEquals(2, refreshed.metadataVersion)
        assertEquals(2, cache.route("order-1").activeWriteVersion)
    }

    @Test
    fun `routing cache records refresh and cache hit metrics`() {
        val registry = SimpleMeterRegistry()
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            client = ScriptedRoutingClient(routing(version = 1, activeWriteVersion = 1, shardCount = 2)),
            refreshInterval = Duration.ofMinutes(5),
            metrics = MicrometerRedisStreamProducerMetrics(registry, "orders", "orders-consumer"),
        )

        cache.route("order-1")
        cache.route("order-2")

        assertEquals(
            1.0,
            registry.get("redis_stream_producer_routing_refresh_total").tag("status", "SUCCESS").counter().count(),
        )
        assertEquals(1.0, registry.get("redis_stream_producer_routing_cache_hit_total").counter().count())
    }

    @Test
    fun `unsupported producer hash algorithm is rejected`() {
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            client = ScriptedRoutingClient(
                routing(version = 1, activeWriteVersion = 1, shardCount = 2, hashAlgorithm = "sha256"),
            ),
        )

        assertFailsWith<IllegalArgumentException> {
            cache.route("order-1")
        }
    }

    @Test
    fun `unbiased murmur3 hash retries modulo tail instead of returning legacy modulo shard`() {
        val shardCount = 1_500_000_001
        val legacy = hashOnlyRouting(shardCount = shardCount, hashAlgorithm = RedisStreamHashAlgorithms.MURMUR3_32)
        val unbiased = hashOnlyRouting(shardCount = shardCount, hashAlgorithm = RedisStreamHashAlgorithms.MURMUR3_32_UNBIASED)
        val key = (0..10_000)
            .map { "order-$it" }
            .firstOrNull {
                RedisStreamPartitionHasher.shardIndex(legacy, it) !=
                    RedisStreamPartitionHasher.shardIndex(unbiased, it)
            } ?: error("test fixture did not find a hash in the rejected modulo tail")

        val legacyShard = RedisStreamPartitionHasher.shardIndex(legacy, key)
        val unbiasedShard = RedisStreamPartitionHasher.shardIndex(unbiased, key)

        assertNotEquals(legacyShard, unbiasedShard)
        assertTrue(unbiasedShard in 0 until shardCount)
    }

    @Test
    fun `routing metadata for a different group is rejected`() {
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            client = ScriptedRoutingClient(
                routing(version = 1, activeWriteVersion = 1, shardCount = 2, consumerGroup = "other-consumer"),
            ),
        )

        assertFailsWith<IllegalArgumentException> {
            cache.route("order-1")
        }
    }

    @Test
    fun `routing metadata must include each active shard index exactly once`() {
        val metadata = routing(version = 1, activeWriteVersion = 1, shardCount = 2)
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            client = ScriptedRoutingClient(
                metadata.copy(
                    shards = listOf(
                        ProducerRoutingShard(1, 0, "orders:v1:shard:0", 0),
                        ProducerRoutingShard(1, 0, "orders:v1:shard:0", 0),
                    ),
                ),
            ),
        )

        assertFailsWith<IllegalArgumentException> {
            cache.route("order-1")
        }
    }

    private fun routing(
        version: Long,
        activeWriteVersion: Int,
        shardCount: Int,
        hashAlgorithm: String = "murmur3",
        streamPrefix: String = "orders",
        consumerGroup: String = "orders-consumer",
    ): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            metadataVersion = version,
            activeWriteVersion = activeWriteVersion,
            shardCount = shardCount,
            hashAlgorithm = hashAlgorithm,
            hashSeed = "default",
            streamKeyPattern = "$streamPrefix:v{streamVersion}:shard:{shardIndex}",
            shards = (0 until shardCount).map { shardIndex ->
                ProducerRoutingShard(
                    streamVersion = activeWriteVersion,
                    shardIndex = shardIndex,
                    streamKey = "$streamPrefix:v$activeWriteVersion:shard:$shardIndex",
                    redisSlot = shardIndex,
                )
            },
        )

    private fun hashOnlyRouting(
        shardCount: Int,
        hashAlgorithm: String,
    ): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            metadataVersion = 1,
            activeWriteVersion = 1,
            shardCount = shardCount,
            hashAlgorithm = hashAlgorithm,
            hashSeed = "default",
            streamKeyPattern = "orders:v{streamVersion}:shard:{shardIndex}",
            shards = emptyList(),
        )
}

private class ScriptedRoutingClient(
    private vararg val responses: ProducerRoutingResponse,
) : CoordinatorClient {
    var calls = 0
        private set

    override fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse =
        HeartbeatResponse(
            responseTo = request.requestId,
            status = com.redisstream.consumer.HeartbeatStatus.RETRY,
            memberId = memberId,
            memberEpoch = request.memberEpoch,
            heartbeatIntervalMs = 0,
            groupEpoch = 0,
            assignmentEpoch = 0,
            metadataVersion = 0,
            assignedMaxConcurrency = 0,
            assignment = AssignmentView(emptySet(), emptySet(), 0),
        )

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        responses[calls++]
}

private class MutableClock(
    private var current: Instant,
) : Clock() {
    override fun getZone(): ZoneId = ZoneId.of("UTC")

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
        current = current.plus(duration)
    }
}
