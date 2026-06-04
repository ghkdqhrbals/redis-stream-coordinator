package com.redisstream.producer

import com.redisstream.consumer.AssignmentView
import com.redisstream.consumer.CoordinatorClient
import com.redisstream.consumer.HeartbeatRequest
import com.redisstream.consumer.HeartbeatResponse
import com.redisstream.consumer.ProducerRoutingResponse
import com.redisstream.consumer.ProducerRoutingShard
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ProducerRoutingCacheTest {
    @Test
    fun `route reuses cached producer routing metadata before refresh interval expires`() {
        val client = ScriptedRoutingClient(routing(version = 1, shardCount = 2))
        val clock = MutableClock(Instant.parse("2026-05-23T00:00:00Z"))
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = client,
            refreshInterval = Duration.ofSeconds(30),
            clock = clock,
        )

        val first = cache.route("order-1")
        clock.advance(Duration.ofSeconds(5))
        val second = cache.route("order-2")

        assertEquals(1, client.calls)
        assertEquals(1, first.metadataVersion)
        assertEquals(1, second.metadataVersion)
        assertEquals(1, cache.cachedMetadataVersion())
    }

    @Test
    fun `route refreshes expired cache and replaces metadata when metadata version changes`() {
        val client = ScriptedRoutingClient(
            routing(version = 1, shardCount = 2),
            routing(version = 2, shardCount = 4),
        )
        val clock = MutableClock(Instant.parse("2026-05-23T00:00:00Z"))
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = client,
            refreshInterval = Duration.ofSeconds(10),
            clock = clock,
        )

        val before = cache.route("order-1")
        clock.advance(Duration.ofSeconds(11))
        val after = cache.route("order-1")

        assertEquals(2, client.calls)
        assertEquals(1, before.metadataVersion)
        assertEquals(2, after.metadataVersion)
        assertEquals(2, after.metadataVersion)
        assertEquals(2, cache.cachedMetadataVersion())
    }

    @Test
    fun `forced refresh checks coordinator even before refresh interval expires`() {
        val client = ScriptedRoutingClient(
            routing(version = 1, shardCount = 2),
            routing(version = 2, shardCount = 3),
        )
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = client,
            refreshInterval = Duration.ofMinutes(5),
        )

        cache.route("order-1")
        val refreshed = cache.refresh()

        assertEquals(2, client.calls)
        assertEquals(2, refreshed.metadataVersion)
        assertEquals(2, cache.route("order-1").metadataVersion)
    }

    @Test
    fun `producer partition hasher is deterministic and bounded by shard count`() {
        val shardCount = 1_500_000_001
        val metadata = hashOnlyRouting(shardCount = shardCount)

        val first = RedisStreamPartitionHasher.shardIndex(metadata, "order-1")
        val second = RedisStreamPartitionHasher.shardIndex(metadata, "order-1")

        assertEquals(first, second)
        assertTrue(first in 0 until shardCount)
    }

    @Test
    fun `routing metadata for a different group is rejected`() {
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = ScriptedRoutingClient(
                routing(version = 1, shardCount = 2, consumerGroup = "other-consumer"),
            ),
        )

        assertFailsWith<IllegalArgumentException> {
            cache.route("order-1")
        }
    }

    @Test
    fun `routing metadata must include each active shard index exactly once`() {
        val metadata = routing(version = 1, shardCount = 2)
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = ScriptedRoutingClient(
                metadata.copy(
                    shards = listOf(
                        ProducerRoutingShard(0, "orders:0", 0),
                        ProducerRoutingShard(0, "orders:0", 0),
                    ),
                ),
            ),
        )

        assertFailsWith<IllegalArgumentException> {
            cache.route("order-1")
        }
    }

    @Test
    fun `initial routing validation fails when coordinator has no active shards`() {
        val cache = ProducerRoutingCache(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = ScriptedRoutingClient(
                routing(version = 1, shardCount = 0),
            ),
        )

        val error = assertFailsWith<IllegalArgumentException> {
            cache.validateInitialRouting()
        }
        assertTrue(error.message!!.contains("has no active shards"))
    }

    private fun routing(
        version: Long,
        shardCount: Int,
        streamPrefix: String = "orders",
        consumerGroup: String = "orders-consumer",
    ): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            metadataVersion = version,
            shardCount = shardCount,
            streamKeyPattern = "$streamPrefix:{shardIndex}",
            shards = (0 until shardCount).map { shardIndex ->
                ProducerRoutingShard(
                    shardIndex = shardIndex,
                    streamKey = "$streamPrefix:$shardIndex",
                    redisSlot = shardIndex,
                )
            },
        )

    private fun hashOnlyRouting(shardCount: Int): ProducerRoutingResponse =
        ProducerRoutingResponse(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            metadataVersion = 1,
            shardCount = shardCount,
            streamKeyPattern = "orders:{shardIndex}",
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
