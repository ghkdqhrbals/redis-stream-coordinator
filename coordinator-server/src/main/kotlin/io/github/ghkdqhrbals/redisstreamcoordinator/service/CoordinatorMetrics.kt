package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupKey
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatStatus
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MemberState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MigrationState
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import org.springframework.beans.factory.ObjectProvider
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

interface CoordinatorMetrics {
    fun recordHealth(up: Boolean)
    fun recordHeartbeat(streamPrefix: String, consumerGroup: String, status: HeartbeatStatus)
    fun recordMemberExpired(group: GroupMetadata, count: Int)
    fun recordRebalance(group: GroupMetadata, reason: String, duration: Duration)
    fun recordScaleRequest(streamPrefix: String, consumerGroup: String, status: String)
    fun recordConsumerConcurrencyUpdate(streamPrefix: String, consumerGroup: String, status: String)
    fun recordTick(result: CoordinatorTickResult, duration: Duration)
    fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int)
}

object NoopCoordinatorMetrics : CoordinatorMetrics {
    override fun recordHealth(up: Boolean) = Unit
    override fun recordHeartbeat(streamPrefix: String, consumerGroup: String, status: HeartbeatStatus) = Unit
    override fun recordMemberExpired(group: GroupMetadata, count: Int) = Unit
    override fun recordRebalance(group: GroupMetadata, reason: String, duration: Duration) = Unit
    override fun recordScaleRequest(streamPrefix: String, consumerGroup: String, status: String) = Unit
    override fun recordConsumerConcurrencyUpdate(streamPrefix: String, consumerGroup: String, status: String) = Unit
    override fun recordTick(result: CoordinatorTickResult, duration: Duration) = Unit
    override fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int) = Unit
}

@Component
class AutoConfiguredCoordinatorMetrics(
    meterRegistry: ObjectProvider<MeterRegistry>,
    properties: CoordinatorProperties,
    clock: Clock,
) : CoordinatorMetrics {
    private val delegate: CoordinatorMetrics =
        meterRegistry.ifAvailable?.let { MicrometerCoordinatorMetrics(it, properties, clock) }
            ?: NoopCoordinatorMetrics

    override fun recordHealth(up: Boolean) = delegate.recordHealth(up)
    override fun recordHeartbeat(streamPrefix: String, consumerGroup: String, status: HeartbeatStatus) =
        delegate.recordHeartbeat(streamPrefix, consumerGroup, status)

    override fun recordMemberExpired(group: GroupMetadata, count: Int) =
        delegate.recordMemberExpired(group, count)

    override fun recordRebalance(group: GroupMetadata, reason: String, duration: Duration) =
        delegate.recordRebalance(group, reason, duration)

    override fun recordScaleRequest(streamPrefix: String, consumerGroup: String, status: String) =
        delegate.recordScaleRequest(streamPrefix, consumerGroup, status)

    override fun recordConsumerConcurrencyUpdate(streamPrefix: String, consumerGroup: String, status: String) =
        delegate.recordConsumerConcurrencyUpdate(streamPrefix, consumerGroup, status)

    override fun recordTick(result: CoordinatorTickResult, duration: Duration) =
        delegate.recordTick(result, duration)

    override fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int) =
        delegate.recordGroupState(group, invariantViolationCount)
}

class MicrometerCoordinatorMetrics(
    private val registry: MeterRegistry,
    private val properties: CoordinatorProperties,
    private val clock: Clock = Clock.systemUTC(),
) : CoordinatorMetrics {
    private val coordinatorTags = Tags.of("coordinator", properties.id)
    private val up = AtomicInteger(1)
    private val groups = ConcurrentHashMap<GroupKey, GroupMeters>()

    init {
        Gauge.builder("redis_stream_coord_up", up) { it.get().toDouble() }
            .tags(coordinatorTags)
            .register(registry)
    }

    override fun recordHealth(up: Boolean) {
        this.up.set(if (up) 1 else 0)
    }

    override fun recordHeartbeat(streamPrefix: String, consumerGroup: String, status: HeartbeatStatus) {
        registry.counter(
            "redis_stream_coord_heartbeat_total",
            groupTags(streamPrefix, consumerGroup).and("status", status.name),
        ).increment()
    }

    override fun recordMemberExpired(group: GroupMetadata, count: Int) {
        if (count > 0) {
            registry.counter("redis_stream_coord_member_expired_total", groupTags(group)).increment(count.toDouble())
        }
    }

    override fun recordRebalance(group: GroupMetadata, reason: String, duration: Duration) {
        val tags = groupTags(group).and("reason", reason)
        registry.counter("redis_stream_coord_rebalance_total", tags).increment()
        registry.timer("redis_stream_coord_rebalance_duration", tags).record(duration)
    }

    override fun recordScaleRequest(streamPrefix: String, consumerGroup: String, status: String) {
        val tags = groupTags(streamPrefix, consumerGroup).and("status", status)
        registry.counter("redis_stream_coord_scale_request_total", tags).increment()
        if (status != "SUCCESS" && status != "NOOP") {
            registry.counter("redis_stream_coord_scale_request_failed_total", tags).increment()
        }
    }

    override fun recordConsumerConcurrencyUpdate(streamPrefix: String, consumerGroup: String, status: String) {
        registry.counter(
            "redis_stream_coord_consumer_concurrency_update_total",
            groupTags(streamPrefix, consumerGroup).and("status", status),
        ).increment()
    }

    override fun recordTick(result: CoordinatorTickResult, duration: Duration) {
        val tags = coordinatorTags.and("changed", (result.changedGroups > 0).toString())
        registry.counter("redis_stream_coord_tick_total", tags).increment()
        registry.timer("redis_stream_coord_tick_duration", tags).record(duration)
    }

    override fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int) {
        val meters = groups.computeIfAbsent(group.key()) { createGroupMeters(group) }
        meters.groupEpoch.set(group.groupEpoch)
        meters.assignmentEpoch.set(group.assignmentEpoch)
        meters.membersTotal.set(group.members.size.toLong())
        meters.membersActive.set(group.members.values.count { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }.toLong())
        meters.membersExpired.set(group.members.values.count { it.state == MemberState.EXPIRED }.toLong())
        meters.migrationActive.set(if (group.activeMigrationId != null) 1 else 0)
        meters.activeMigrationAgeSeconds.set(group.activeMigrationAgeSeconds())
        meters.revokePending.set(group.members.values.sumOf { it.revoking.size }.toLong())
        meters.invariantViolations.set(invariantViolationCount.toLong())
        if (invariantViolationCount > 0) {
            registry.counter("redis_stream_coord_invariant_violation_total", groupTags(group))
                .increment(invariantViolationCount.toDouble())
        }
    }

    private fun createGroupMeters(group: GroupMetadata): GroupMeters {
        val tags = groupTags(group)
        val meters = GroupMeters()
        registerGauge("redis_stream_coord_group_epoch", tags, meters.groupEpoch)
        registerGauge("redis_stream_coord_assignment_epoch", tags, meters.assignmentEpoch)
        registerGauge("redis_stream_coord_members", tags.and("state", "total"), meters.membersTotal)
        registerGauge("redis_stream_coord_members", tags.and("state", "active"), meters.membersActive)
        registerGauge("redis_stream_coord_members", tags.and("state", "expired"), meters.membersExpired)
        registerGauge("redis_stream_coord_migration_active", tags, meters.migrationActive)
        registerGauge("redis_stream_coord_migration_active_age_seconds", tags, meters.activeMigrationAgeSeconds)
        registerGauge("redis_stream_coord_revoke_pending", tags, meters.revokePending)
        registerGauge("redis_stream_coord_invariant_violations", tags, meters.invariantViolations)
        return meters
    }

    private fun registerGauge(name: String, tags: Tags, value: AtomicLong) {
        Gauge.builder(name, value) { it.get().toDouble() }
            .tags(tags)
            .register(registry)
    }

    private fun GroupMetadata.activeMigrationAgeSeconds(): Long {
        val migration = activeMigrationId?.let { migrations[it] } ?: return 0
        if (migration.state == MigrationState.DEPRECATED || migration.state == MigrationState.ROLLED_BACK) {
            return 0
        }
        return Duration.between(migration.createdAt, Instant.now(clock)).seconds.coerceAtLeast(0)
    }

    private fun groupTags(group: GroupMetadata): Tags =
        groupTags(group.streamPrefix, group.consumerGroup)

    private fun groupTags(streamPrefix: String, consumerGroup: String): Tags =
        coordinatorTags.and("stream", streamPrefix, "group", consumerGroup)

    private fun GroupMetadata.key(): GroupKey =
        GroupKey(streamPrefix, consumerGroup)

    private class GroupMeters {
        val groupEpoch = AtomicLong(0)
        val assignmentEpoch = AtomicLong(0)
        val membersTotal = AtomicLong(0)
        val membersActive = AtomicLong(0)
        val membersExpired = AtomicLong(0)
        val migrationActive = AtomicLong(0)
        val activeMigrationAgeSeconds = AtomicLong(0)
        val revokePending = AtomicLong(0)
        val invariantViolations = AtomicLong(0)
    }
}
