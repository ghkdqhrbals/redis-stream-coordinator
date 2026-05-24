package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RuntimeConsumerCapacity
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ScaleGroupRequest
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
            hashAlgorithm = "murmur3",
            requestedBy = "test",
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId> = emptySet(),
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
