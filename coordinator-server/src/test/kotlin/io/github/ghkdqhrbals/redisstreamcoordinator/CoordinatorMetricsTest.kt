package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RuntimeConsumerCapacity
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ScaleGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardConsumptionProgress
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamShardOffset
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamShardOffsetsResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorMetrics
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.service.MicrometerCoordinatorMetrics
import io.github.ghkdqhrbals.redisstreamcoordinator.store.InMemoryCoordinatorStateStore
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.NoopStreamShardProvisioner
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class CoordinatorMetricsTest {
    private val clock = MetricsMutableClock(Instant.parse("2026-05-23T00:00:00Z"), ZoneOffset.UTC)
    private val properties = CoordinatorProperties(
        id = "metrics-coordinator",
        heartbeatInterval = Duration.ofSeconds(3),
        memberLeaseTtl = Duration.ofSeconds(15),
        defaults = CoordinatorProperties.Defaults(initialShardCount = 2, consumerMaxConcurrency = 4),
    )

    @Test
    fun `coordinator records heartbeat group state and expiry metrics`() {
        val registry = SimpleMeterRegistry()
        val service = service(MicrometerCoordinatorMetrics(registry, properties, clock))

        service.createGroup("metrics-orders", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat(
            "metrics-orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "metrics-orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        assertEquals(
            2.0,
            registry.get("redis_stream_coord_heartbeat_total").tag("status", "OK").counter().count(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_coord_members")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("state", "active")
                .gauge()
                .value(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_coord_member_active")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                .gauge()
                .value(),
        )
        assertEquals(
            0.0,
            registry.get("redis_stream_coord_member_heartbeat_age_seconds")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                .gauge()
                .value(),
        )
        assertEquals(
            15.0,
            registry.get("redis_stream_coord_member_lease_remaining_seconds")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                .gauge()
                .value(),
        )
        assertEquals(
            4.0,
            registry.get("redis_stream_coord_member_runtime_max_concurrency")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                .gauge()
                .value(),
        )
        assertEquals(
            0.0,
            registry.get("redis_stream_coord_member_active_workers")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                .gauge()
                .value(),
        )
        assertEquals(
            2.0,
            registry.get("redis_stream_coord_member_current_shards")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                .gauge()
                .value(),
        )

        clock.advance(Duration.ofSeconds(16))
        val tick = service.tick()

        assertEquals(1, tick.scannedGroups)
        assertEquals(1, tick.changedGroups)
        assertEquals(1.0, registry.get("redis_stream_coord_member_expired_total").counter().count())
        assertEquals(
            1.0,
            registry.get("redis_stream_coord_members")
                .tag("stream", "metrics-orders")
                .tag("group", "orders-consumer")
                .tag("state", "expired")
                .gauge()
                .value(),
        )
        assertEquals(1.0, registry.get("redis_stream_coord_tick_total").counter().count())
    }

    @Test
    fun `coordinator records producer routing metrics`() {
        val registry = SimpleMeterRegistry()
        val service = service(MicrometerCoordinatorMetrics(registry, properties, clock))

        service.createGroup("metrics-routing", "orders-consumer", createGroupRequest(initialShardCount = 2))
        service.producerRouting("metrics-routing", "orders-consumer")
        assertFailsWith<RuntimeException> {
            service.producerRouting("metrics-routing", "missing-group")
        }

        assertEquals(
            1.0,
            registry.get("redis_stream_coord_producer_routing_request_total")
                .tag("stream", "metrics-routing")
                .tag("group", "orders-consumer")
                .tag("status", "SUCCESS")
                .counter()
                .count(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_coord_producer_routing_request_total")
                .tag("stream", "metrics-routing")
                .tag("group", "missing-group")
                .tag("status", "ERROR")
                .counter()
                .count(),
        )
    }

    @Test
    fun `coordinator records scale migration and rebalance metrics`() {
        val registry = SimpleMeterRegistry()
        val service = service(MicrometerCoordinatorMetrics(registry, properties, clock))

        service.createGroup("metrics-scale", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat(
            "metrics-scale",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "metrics-scale",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        service.scaleGroup(
            "metrics-scale",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "metrics"),
        )

        assertEquals(
            1.0,
            registry.get("redis_stream_coord_scale_request_total").tag("status", "SUCCESS").counter().count(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_coord_migration_active")
                .tag("stream", "metrics-scale")
                .tag("group", "orders-consumer")
                .gauge()
                .value(),
        )
        assertTrue(registry.get("redis_stream_coord_rebalance_total").counter().count() >= 1.0)
    }

    @Test
    fun `coordinator records state conflict retry metrics`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerCoordinatorMetrics(registry, properties, clock)

        metrics.recordStateConflict("heartbeat", attempt = 1)

        assertEquals(
            1.0,
            registry.get("redis_stream_coord_state_conflict_total")
                .tag("operation", "heartbeat")
                .tag("attempt", "1")
                .counter()
                .count(),
        )
    }

    @Test
    fun `coordinator exports consumer shard progress metrics`() {
        val registry = SimpleMeterRegistry()
        val service = service(MicrometerCoordinatorMetrics(registry, properties, clock))

        service.createGroup("metrics-progress", "orders-consumer", createGroupRequest(initialShardCount = 1))
        val first = service.heartbeat(
            "metrics-progress",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val shard = first.assignment.assignedShards.single()
        service.heartbeat(
            "metrics-progress",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = first.memberEpoch,
                ownedShards = setOf(shard),
                shardProgress = listOf(
                    ShardConsumptionProgress(
                        shard = shard,
                        streamKey = "metrics-progress:0",
                        lastDeliveredId = "100-2",
                        lastAckedId = "100-1",
                        pendingCount = 1,
                        updatedAt = Instant.parse("2026-05-22T23:59:55Z"),
                    ),
                ),
            ),
        )

        assertEquals(
            100.0,
            registry.get("redis_stream_coord_consumer_shard_last_acked_ms")
                .tag("stream", "metrics-progress")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                                .tag("shard", "0")
                .gauge()
                .value(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_coord_consumer_shard_pending")
                .tag("stream", "metrics-progress")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                                .tag("shard", "0")
                .gauge()
                .value(),
        )
        assertEquals(
            5.0,
            registry.get("redis_stream_coord_consumer_shard_progress_age_seconds")
                .tag("stream", "metrics-progress")
                .tag("group", "orders-consumer")
                .tag("member", "member-a")
                                .tag("shard", "0")
                .gauge()
                .value(),
        )
    }

    @Test
    fun `coordinator exports stream shard offset and lag metrics`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerCoordinatorMetrics(registry, properties, clock)

        metrics.recordStreamShardOffsets(
            StreamShardOffsetsResponse(
                streamPrefix = "metrics-offsets",
                consumerGroup = "orders-consumer",
                shards = listOf(
                    StreamShardOffset(
                        streamPrefix = "metrics-offsets",
                        consumerGroup = "orders-consumer",
                        shard = ShardId(shardIndex = 0),
                        streamKey = "metrics-offsets:0",
                        redisSlot = 0,
                        redisNodeEndpoint = null,
                        redisNodeId = null,
                        redisSlotRangeStart = null,
                        redisSlotRangeEnd = null,
                        streamLength = 42,
                        firstRecordId = "100-0",
                        lastRecordId = "200-3",
                        lastGeneratedId = "200-3",
                        groupLastDeliveredId = "190-0",
                        consumerLastDeliveredId = "190-0",
                        consumerLastAckedId = "180-1",
                        pendingCount = 3,
                        lag = 7,
                        memoryUsageBytes = 2048,
                        ownerMemberIds = listOf("member-a"),
                    ),
                ),
                totalStreamLength = 42,
                totalPendingCount = 3,
                totalLag = 7,
                totalMemoryUsageBytes = 2048,
                memoryUsageKnown = true,
            ),
        )

        assertEquals(
            42.0,
            registry.get("redis_stream_coord_shard_stream_length")
                .tag("stream", "metrics-offsets")
                .tag("group", "orders-consumer")
                                .tag("shard", "0")
                .tag("stream_key", "metrics-offsets:0")
                .gauge()
                .value(),
        )
        assertEquals(
            7.0,
            registry.get("redis_stream_coord_shard_lag")
                .tag("stream", "metrics-offsets")
                .tag("group", "orders-consumer")
                .tag("shard", "0")
                .gauge()
                .value(),
        )
        assertEquals(
            2048.0,
            registry.get("redis_stream_coord_shard_memory_usage_bytes")
                .tag("stream", "metrics-offsets")
                .tag("group", "orders-consumer")
                .tag("shard", "0")
                .gauge()
                .value(),
        )
        assertEquals(
            180.0,
            registry.get("redis_stream_coord_shard_consumer_last_acked_ms")
                .tag("stream", "metrics-offsets")
                .tag("group", "orders-consumer")
                .tag("shard", "0")
                .gauge()
                .value(),
        )
        assertEquals(
            1.0,
            registry.get("redis_stream_coord_shard_consumer_last_acked_seq")
                .tag("stream", "metrics-offsets")
                .tag("group", "orders-consumer")
                .tag("shard", "0")
                .gauge()
                .value(),
        )
    }

    @Test
    fun `coordinator exports unknown Redis lag as zero in Prometheus gauge`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerCoordinatorMetrics(registry, properties, clock)

        metrics.recordStreamShardOffsets(
            StreamShardOffsetsResponse(
                streamPrefix = "metrics-unknown-lag",
                consumerGroup = "orders-consumer",
                shards = listOf(
                    StreamShardOffset(
                        streamPrefix = "metrics-unknown-lag",
                        consumerGroup = "orders-consumer",
                        shard = ShardId(shardIndex = 0),
                        streamKey = "metrics-unknown-lag:0",
                        redisSlot = 0,
                        redisNodeEndpoint = null,
                        redisNodeId = null,
                        redisSlotRangeStart = null,
                        redisSlotRangeEnd = null,
                        streamLength = 0,
                        firstRecordId = null,
                        lastRecordId = null,
                        lastGeneratedId = null,
                        groupLastDeliveredId = null,
                        consumerLastDeliveredId = null,
                        consumerLastAckedId = null,
                        pendingCount = 0,
                        lag = null,
                        memoryUsageBytes = null,
                        ownerMemberIds = emptyList(),
                    ),
                ),
                totalStreamLength = 0,
                totalPendingCount = 0,
                totalLag = null,
                totalMemoryUsageBytes = 0,
                memoryUsageKnown = false,
            ),
        )

        assertEquals(
            0.0,
            registry.get("redis_stream_coord_shard_lag")
                .tag("stream", "metrics-unknown-lag")
                .tag("group", "orders-consumer")
                .tag("shard", "0")
                .gauge()
                .value(),
        )
    }

    @Test
    fun `coordinator records API request latency metrics`() {
        val registry = SimpleMeterRegistry()
        val metrics = MicrometerCoordinatorMetrics(registry, properties, clock)

        metrics.recordApiRequest(
            method = "GET",
            route = "/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/offsets",
            status = 200,
            outcome = "SUCCESS",
            streamPrefix = "metrics-api",
            consumerGroup = "orders-consumer",
            duration = Duration.ofMillis(3250),
        )

        assertEquals(
            1.0,
            registry.get("redis_stream_coord_api_request_total")
                .tag("method", "GET")
                .tag("route", "/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/offsets")
                .tag("status", "200")
                .tag("outcome", "SUCCESS")
                .tag("stream", "metrics-api")
                .tag("group", "orders-consumer")
                .counter()
                .count(),
        )
        assertEquals(
            3.25,
            registry.get("redis_stream_coord_api_request_duration")
                .tag("route", "/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/offsets")
                .tag("stream", "metrics-api")
                .tag("group", "orders-consumer")
                .timer()
                .totalTime(java.util.concurrent.TimeUnit.SECONDS),
        )
    }

    private fun service(metrics: CoordinatorMetrics): CoordinatorService =
        CoordinatorService(
            properties = properties,
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = NoopStreamShardProvisioner,
            clock = clock,
            metrics = metrics,
        )

    private fun createGroupRequest(initialShardCount: Int): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId> = emptySet(),
        shardProgress: List<ShardConsumptionProgress> = emptyList(),
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
            shardProgress = shardProgress,
        )
}

private class MetricsMutableClock(
    private var current: Instant,
    private val zone: ZoneId,
) : Clock() {
    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock =
        MetricsMutableClock(current, zone)

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
        current = current.plus(duration)
    }
}
