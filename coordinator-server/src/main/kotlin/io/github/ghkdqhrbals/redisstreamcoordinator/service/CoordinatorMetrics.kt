package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupKey
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatStatus
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MemberMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MemberState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MigrationState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardConsumptionProgress
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamShardOffset
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamShardOffsetsResponse
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.Timer
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
    fun recordProducerRouting(streamPrefix: String, consumerGroup: String, status: String)
    fun recordTick(result: CoordinatorTickResult, duration: Duration)
    fun recordStateConflict(operation: String, attempt: Int)
    fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int)
    fun recordStreamShardOffsets(offsets: StreamShardOffsetsResponse)
    fun recordApiRequest(
        method: String,
        route: String,
        status: Int,
        outcome: String,
        streamPrefix: String?,
        consumerGroup: String?,
        duration: Duration,
    )
}

object NoopCoordinatorMetrics : CoordinatorMetrics {
    override fun recordHealth(up: Boolean) = Unit
    override fun recordHeartbeat(streamPrefix: String, consumerGroup: String, status: HeartbeatStatus) = Unit
    override fun recordMemberExpired(group: GroupMetadata, count: Int) = Unit
    override fun recordRebalance(group: GroupMetadata, reason: String, duration: Duration) = Unit
    override fun recordScaleRequest(streamPrefix: String, consumerGroup: String, status: String) = Unit
    override fun recordProducerRouting(streamPrefix: String, consumerGroup: String, status: String) = Unit
    override fun recordTick(result: CoordinatorTickResult, duration: Duration) = Unit
    override fun recordStateConflict(operation: String, attempt: Int) = Unit
    override fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int) = Unit
    override fun recordStreamShardOffsets(offsets: StreamShardOffsetsResponse) = Unit
    override fun recordApiRequest(
        method: String,
        route: String,
        status: Int,
        outcome: String,
        streamPrefix: String?,
        consumerGroup: String?,
        duration: Duration,
    ) = Unit
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

    override fun recordProducerRouting(streamPrefix: String, consumerGroup: String, status: String) =
        delegate.recordProducerRouting(streamPrefix, consumerGroup, status)

    override fun recordTick(result: CoordinatorTickResult, duration: Duration) =
        delegate.recordTick(result, duration)

    override fun recordStateConflict(operation: String, attempt: Int) =
        delegate.recordStateConflict(operation, attempt)

    override fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int) =
        delegate.recordGroupState(group, invariantViolationCount)

    override fun recordStreamShardOffsets(offsets: StreamShardOffsetsResponse) =
        delegate.recordStreamShardOffsets(offsets)

    override fun recordApiRequest(
        method: String,
        route: String,
        status: Int,
        outcome: String,
        streamPrefix: String?,
        consumerGroup: String?,
        duration: Duration,
    ) = delegate.recordApiRequest(method, route, status, outcome, streamPrefix, consumerGroup, duration)
}

class MicrometerCoordinatorMetrics(
    private val registry: MeterRegistry,
    private val properties: CoordinatorProperties,
    private val clock: Clock = Clock.systemUTC(),
) : CoordinatorMetrics {
    private val coordinatorTags = Tags.of("coordinator", properties.id)
    private val up = AtomicInteger(1)
    private val groups = ConcurrentHashMap<GroupKey, GroupMeters>()
    private val streamOffsets = ConcurrentHashMap<StreamShardMetricKey, StreamShardOffsetMeters>()

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

    override fun recordProducerRouting(streamPrefix: String, consumerGroup: String, status: String) {
        registry.counter(
            "redis_stream_coord_producer_routing_request_total",
            groupTags(streamPrefix, consumerGroup).and("status", status),
        ).increment()
    }

    override fun recordTick(result: CoordinatorTickResult, duration: Duration) {
        val tags = coordinatorTags.and("changed", (result.changedGroups > 0).toString())
        registry.counter("redis_stream_coord_tick_total", tags).increment()
        registry.timer("redis_stream_coord_tick_duration", tags).record(duration)
    }

    override fun recordStateConflict(operation: String, attempt: Int) {
        registry.counter(
            "redis_stream_coord_state_conflict_total",
            coordinatorTags.and("operation", operation, "attempt", attempt.coerceAtLeast(1).toString()),
        ).increment()
    }

    override fun recordApiRequest(
        method: String,
        route: String,
        status: Int,
        outcome: String,
        streamPrefix: String?,
        consumerGroup: String?,
        duration: Duration,
    ) {
        val tags = coordinatorTags.and(
            "method", method.uppercase(),
            "route", route,
            "status", status.toString(),
            "outcome", outcome,
            "stream", streamPrefix.orEmpty().ifBlank { "none" },
            "group", consumerGroup.orEmpty().ifBlank { "none" },
        )
        registry.counter("redis_stream_coord_api_request_total", tags).increment()
        Timer.builder("redis_stream_coord_api_request_duration")
            .description("Coordinator HTTP API request latency")
            .tags(tags)
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry)
            .record(duration)
    }

    override fun recordGroupState(group: GroupMetadata, invariantViolationCount: Int) {
        val meters = groups.computeIfAbsent(group.key()) { createGroupMeters(group) }
        meters.groupEpoch.set(group.groupEpoch)
        meters.assignmentEpoch.set(group.assignmentEpoch)
        meters.membersTotal.set(group.members.size.toLong())
        meters.membersActive.set(group.members.values.count { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }.toLong())
        meters.membersExpired.set(group.members.values.count { it.state == MemberState.EXPIRED }.toLong())
        meters.migrationActive.set(if (group.activeReshardingId != null) 1 else 0)
        meters.activeMigrationAgeSeconds.set(group.activeMigrationAgeSeconds())
        meters.revokePending.set(group.members.values.sumOf { it.revoking.size }.toLong())
        meters.invariantViolations.set(invariantViolationCount.toLong())
        group.members.values.forEach { member ->
            meters.members
                .computeIfAbsent(member.memberId) { createMemberMeters(group, member.memberId) }
                .update(member, clock)
            member.shardProgress.forEach { progress ->
                meters.progress
                    .computeIfAbsent(MemberShardKey(member.memberId, progress.shard)) {
                        createShardProgressMeters(group, member.memberId, progress.shard)
                    }
                    .update(progress, clock)
            }
        }
        if (invariantViolationCount > 0) {
            registry.counter("redis_stream_coord_invariant_violation_total", groupTags(group))
                .increment(invariantViolationCount.toDouble())
        }
    }

    override fun recordStreamShardOffsets(offsets: StreamShardOffsetsResponse) {
        offsets.shards.forEach { offset ->
            streamOffsets
                .computeIfAbsent(StreamShardMetricKey(offset.streamPrefix, offset.consumerGroup, offset.shard)) {
                    createStreamShardOffsetMeters(offset)
                }
                .update(offset)
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

    private fun createMemberMeters(group: GroupMetadata, memberId: String): MemberMeters {
        val tags = groupTags(group).and("member", memberId)
        val meters = MemberMeters()
        registerGauge("redis_stream_coord_member_active", tags, meters.active)
        registerGauge("redis_stream_coord_member_heartbeat_age_seconds", tags, meters.heartbeatAgeSeconds)
        registerGauge("redis_stream_coord_member_lease_remaining_seconds", tags, meters.leaseRemainingSeconds)
        registerGauge("redis_stream_coord_member_runtime_max_concurrency", tags, meters.runtimeMaxConcurrency)
        registerGauge("redis_stream_coord_member_active_workers", tags, meters.activeWorkers)
        registerGauge("redis_stream_coord_member_current_shards", tags, meters.currentShards)
        registerGauge("redis_stream_coord_member_revoking_shards", tags, meters.revokingShards)
        return meters
    }

    private fun createShardProgressMeters(
        group: GroupMetadata,
        memberId: String,
        shard: ShardId,
    ): ShardProgressMeters {
        val tags = groupTags(group)
            .and(
                "member", memberId,
                "shard", shard.shardIndex.toString(),
            )
        val meters = ShardProgressMeters()
        registerGauge("redis_stream_coord_consumer_shard_last_delivered_ms", tags, meters.lastDeliveredMs)
        registerGauge("redis_stream_coord_consumer_shard_last_delivered_seq", tags, meters.lastDeliveredSeq)
        registerGauge("redis_stream_coord_consumer_shard_last_acked_ms", tags, meters.lastAckedMs)
        registerGauge("redis_stream_coord_consumer_shard_last_acked_seq", tags, meters.lastAckedSeq)
        registerGauge("redis_stream_coord_consumer_shard_pending", tags, meters.pending)
        registerGauge("redis_stream_coord_consumer_shard_progress_updated_at_seconds", tags, meters.updatedAtSeconds)
        registerGauge("redis_stream_coord_consumer_shard_progress_age_seconds", tags, meters.progressAgeSeconds)
        return meters
    }

    private fun createStreamShardOffsetMeters(offset: StreamShardOffset): StreamShardOffsetMeters {
        val tags = groupTags(offset.streamPrefix, offset.consumerGroup)
            .and(
                "shard", offset.shard.shardIndex.toString(),
                "stream_key", offset.streamKey,
            )
        val meters = StreamShardOffsetMeters()
        registerGauge("redis_stream_coord_shard_stream_length", tags, meters.streamLength)
        registerGauge("redis_stream_coord_shard_pending", tags, meters.pending)
        registerGauge("redis_stream_coord_shard_lag", tags, meters.lag)
        registerGauge("redis_stream_coord_shard_memory_usage_bytes", tags, meters.memoryUsageBytes)
        registerGauge("redis_stream_coord_shard_last_record_ms", tags, meters.lastRecordMs)
        registerGauge("redis_stream_coord_shard_last_record_seq", tags, meters.lastRecordSeq)
        registerGauge("redis_stream_coord_shard_last_generated_ms", tags, meters.lastGeneratedMs)
        registerGauge("redis_stream_coord_shard_last_generated_seq", tags, meters.lastGeneratedSeq)
        registerGauge("redis_stream_coord_shard_group_last_delivered_ms", tags, meters.groupLastDeliveredMs)
        registerGauge("redis_stream_coord_shard_group_last_delivered_seq", tags, meters.groupLastDeliveredSeq)
        registerGauge("redis_stream_coord_shard_consumer_last_acked_ms", tags, meters.consumerLastAckedMs)
        registerGauge("redis_stream_coord_shard_consumer_last_acked_seq", tags, meters.consumerLastAckedSeq)
        return meters
    }

    private fun registerGauge(name: String, tags: Tags, value: AtomicLong) {
        Gauge.builder(name, value) { it.get().toDouble() }
            .tags(tags)
            .register(registry)
    }

    private fun GroupMetadata.activeMigrationAgeSeconds(): Long {
        val migration = activeReshardingId?.let { migrations[it] } ?: return 0
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
        val members = ConcurrentHashMap<String, MemberMeters>()
        val progress = ConcurrentHashMap<MemberShardKey, ShardProgressMeters>()
    }

    private class MemberMeters {
        val active = AtomicLong(0)
        val heartbeatAgeSeconds = AtomicLong(0)
        val leaseRemainingSeconds = AtomicLong(0)
        val runtimeMaxConcurrency = AtomicLong(0)
        val activeWorkers = AtomicLong(0)
        val currentShards = AtomicLong(0)
        val revokingShards = AtomicLong(0)

        fun update(member: MemberMetadata, clock: Clock) {
            val now = Instant.now(clock)
            active.set(if (member.state == MemberState.ACTIVE || member.state == MemberState.STARTING) 1 else 0)
            heartbeatAgeSeconds.set(Duration.between(member.lastHeartbeatAt, now).seconds.coerceAtLeast(0))
            leaseRemainingSeconds.set(Duration.between(now, member.memberLeaseExpiresAt).seconds.coerceAtLeast(0))
            runtimeMaxConcurrency.set(member.runtimeMaxConcurrency.toLong())
            activeWorkers.set(member.activeConsumerWorkers.toLong())
            currentShards.set(member.currentAssignment.size.toLong())
            revokingShards.set(member.revoking.size.toLong())
        }
    }

    private data class MemberShardKey(
        val memberId: String,
        val shard: ShardId,
    )

    private data class StreamShardMetricKey(
        val streamPrefix: String,
        val consumerGroup: String,
        val shard: ShardId,
    )

    private class ShardProgressMeters {
        val lastDeliveredMs = AtomicLong(-1)
        val lastDeliveredSeq = AtomicLong(-1)
        val lastAckedMs = AtomicLong(-1)
        val lastAckedSeq = AtomicLong(-1)
        val pending = AtomicLong(0)
        val updatedAtSeconds = AtomicLong(0)
        val progressAgeSeconds = AtomicLong(0)

        fun update(progress: ShardConsumptionProgress, clock: Clock) {
            val delivered = progress.lastDeliveredId.toRedisStreamIdParts()
            val acked = progress.lastAckedId.toRedisStreamIdParts()
            lastDeliveredMs.set(delivered.first)
            lastDeliveredSeq.set(delivered.second)
            lastAckedMs.set(acked.first)
            lastAckedSeq.set(acked.second)
            pending.set(progress.pendingCount.coerceAtLeast(0))
            updatedAtSeconds.set(progress.updatedAt?.epochSecond ?: 0)
            progressAgeSeconds.set(
                progress.updatedAt
                    ?.let { Duration.between(it, Instant.now(clock)).seconds.coerceAtLeast(0) }
                    ?: 0,
            )
        }

        private fun String?.toRedisStreamIdParts(): Pair<Long, Long> {
            if (this == null) {
                return -1L to -1L
            }
            val milliseconds = substringBefore("-").toLongOrNull() ?: -1L
            val sequence = substringAfter("-", "").toLongOrNull() ?: -1L
            return milliseconds to sequence
        }
    }

    private class StreamShardOffsetMeters {
        val streamLength = AtomicLong(0)
        val pending = AtomicLong(0)
        val lag = AtomicLong(0)
        val memoryUsageBytes = AtomicLong(0)
        val lastRecordMs = AtomicLong(-1)
        val lastRecordSeq = AtomicLong(-1)
        val lastGeneratedMs = AtomicLong(-1)
        val lastGeneratedSeq = AtomicLong(-1)
        val groupLastDeliveredMs = AtomicLong(-1)
        val groupLastDeliveredSeq = AtomicLong(-1)
        val consumerLastAckedMs = AtomicLong(-1)
        val consumerLastAckedSeq = AtomicLong(-1)

        fun update(offset: StreamShardOffset) {
            val lastRecord = offset.lastRecordId.toRedisStreamIdParts()
            val lastGenerated = offset.lastGeneratedId.toRedisStreamIdParts()
            val groupDelivered = offset.groupLastDeliveredId.toRedisStreamIdParts()
            val consumerAcked = offset.consumerLastAckedId.toRedisStreamIdParts()
            streamLength.set(offset.streamLength.coerceAtLeast(0))
            pending.set(offset.pendingCount.coerceAtLeast(0))
            lag.set((offset.lag ?: 0L).coerceAtLeast(0))
            memoryUsageBytes.set((offset.memoryUsageBytes ?: 0L).coerceAtLeast(0))
            lastRecordMs.set(lastRecord.first)
            lastRecordSeq.set(lastRecord.second)
            lastGeneratedMs.set(lastGenerated.first)
            lastGeneratedSeq.set(lastGenerated.second)
            groupLastDeliveredMs.set(groupDelivered.first)
            groupLastDeliveredSeq.set(groupDelivered.second)
            consumerLastAckedMs.set(consumerAcked.first)
            consumerLastAckedSeq.set(consumerAcked.second)
        }

        private fun String?.toRedisStreamIdParts(): Pair<Long, Long> {
            if (this == null) {
                return -1L to -1L
            }
            val milliseconds = substringBefore("-").toLongOrNull() ?: -1L
            val sequence = substringAfter("-", "").toLongOrNull() ?: -1L
            return milliseconds to sequence
        }
    }
}
