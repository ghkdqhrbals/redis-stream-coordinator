package com.redisstream.consumer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

interface CoordinatorConsumerMetrics {
    fun recordHeartbeat(status: HeartbeatStatus, duration: Duration)
    fun recordLeave(status: HeartbeatStatus?, duration: Duration)
    fun recordAssignment(assignedShards: Int, pendingShards: Int, revokingShards: Int)
    fun recordRevoked(shards: Int)
    fun recordFenced()
    fun recordMessageHandled(status: String, duration: Duration)
    fun recordMessageAck()
}

object NoopCoordinatorConsumerMetrics : CoordinatorConsumerMetrics {
    override fun recordHeartbeat(status: HeartbeatStatus, duration: Duration) = Unit
    override fun recordLeave(status: HeartbeatStatus?, duration: Duration) = Unit
    override fun recordAssignment(assignedShards: Int, pendingShards: Int, revokingShards: Int) = Unit
    override fun recordRevoked(shards: Int) = Unit
    override fun recordFenced() = Unit
    override fun recordMessageHandled(status: String, duration: Duration) = Unit
    override fun recordMessageAck() = Unit
}

class MicrometerCoordinatorConsumerMetrics(
    private val registry: MeterRegistry,
    streamPrefix: String,
    consumerGroup: String,
    memberName: String,
) : CoordinatorConsumerMetrics {
    private val tags = Tags.of(
        "stream", streamPrefix.ifBlank { "unknown" },
        "group", consumerGroup.ifBlank { "unknown" },
        "member", memberName.ifBlank { "unknown" },
    )
    private val assignedShards = AtomicInteger(0)
    private val pendingShards = AtomicInteger(0)
    private val revokingShards = AtomicInteger(0)

    init {
        Gauge.builder("redis_stream_consumer_assigned_shards", assignedShards) { it.get().toDouble() }
            .tags(tags)
            .register(registry)
        Gauge.builder("redis_stream_consumer_pending_shards", pendingShards) { it.get().toDouble() }
            .tags(tags)
            .register(registry)
        Gauge.builder("redis_stream_consumer_revoking_shards", revokingShards) { it.get().toDouble() }
            .tags(tags)
            .register(registry)
    }

    override fun recordHeartbeat(status: HeartbeatStatus, duration: Duration) {
        registry.counter("redis_stream_consumer_heartbeat_total", tags.and("status", status.name)).increment()
        registry.timer("redis_stream_consumer_heartbeat_duration", tags.and("status", status.name)).record(duration)
    }

    override fun recordLeave(status: HeartbeatStatus?, duration: Duration) {
        val statusTag = status?.name ?: "SKIPPED"
        registry.counter("redis_stream_consumer_leave_total", tags.and("status", statusTag)).increment()
        registry.timer("redis_stream_consumer_leave_duration", tags.and("status", statusTag)).record(duration)
    }

    override fun recordAssignment(assignedShards: Int, pendingShards: Int, revokingShards: Int) {
        this.assignedShards.set(assignedShards)
        this.pendingShards.set(pendingShards)
        this.revokingShards.set(revokingShards)
    }

    override fun recordRevoked(shards: Int) {
        if (shards > 0) {
            registry.counter("redis_stream_consumer_revoked_shards_total", tags).increment(shards.toDouble())
        }
    }

    override fun recordFenced() {
        registry.counter("redis_stream_consumer_fenced_total", tags).increment()
    }

    override fun recordMessageHandled(status: String, duration: Duration) {
        registry.counter("redis_stream_consumer_messages_total", tags.and("status", status)).increment()
        registry.timer("redis_stream_consumer_message_duration", tags.and("status", status)).record(duration)
    }

    override fun recordMessageAck() {
        registry.counter("redis_stream_consumer_ack_total", tags).increment()
    }
}
