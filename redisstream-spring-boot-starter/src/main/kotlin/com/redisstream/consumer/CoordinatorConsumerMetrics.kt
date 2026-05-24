package com.redisstream.consumer

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

interface CoordinatorConsumerMetrics {
    fun recordHeartbeat(status: HeartbeatStatus, duration: Duration)
    fun recordLeave(status: HeartbeatStatus?, duration: Duration)
    fun recordRuntimeCapacity(runtimeMaxConcurrency: Int, availableConcurrency: Int, inFlight: Int)
    fun recordAssignment(assignedShards: Int, pendingShards: Int, revokingShards: Int)
    fun recordRevoked(shards: Int)
    fun recordFenced()
    fun recordMessageHandled(status: String, duration: Duration)
    fun recordMessageAck()
    fun recordMessageAck(status: String) {
        if (status == "SUCCESS") {
            recordMessageAck()
        }
    }
}

object NoopCoordinatorConsumerMetrics : CoordinatorConsumerMetrics {
    override fun recordHeartbeat(status: HeartbeatStatus, duration: Duration) = Unit
    override fun recordLeave(status: HeartbeatStatus?, duration: Duration) = Unit
    override fun recordRuntimeCapacity(runtimeMaxConcurrency: Int, availableConcurrency: Int, inFlight: Int) = Unit
    override fun recordAssignment(assignedShards: Int, pendingShards: Int, revokingShards: Int) = Unit
    override fun recordRevoked(shards: Int) = Unit
    override fun recordFenced() = Unit
    override fun recordMessageHandled(status: String, duration: Duration) = Unit
    override fun recordMessageAck() = Unit
    override fun recordMessageAck(status: String) = Unit
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
    private val runtimeMaxConcurrency = AtomicInteger(0)
    private val availableConcurrency = AtomicInteger(0)
    private val inFlightMessages = AtomicInteger(0)

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
        Gauge.builder("redis_stream_consumer_runtime_max_concurrency", runtimeMaxConcurrency) { it.get().toDouble() }
            .tags(tags)
            .register(registry)
        Gauge.builder("redis_stream_consumer_available_concurrency", availableConcurrency) { it.get().toDouble() }
            .tags(tags)
            .register(registry)
        Gauge.builder("redis_stream_consumer_in_flight_messages", inFlightMessages) { it.get().toDouble() }
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

    override fun recordRuntimeCapacity(runtimeMaxConcurrency: Int, availableConcurrency: Int, inFlight: Int) {
        this.runtimeMaxConcurrency.set(runtimeMaxConcurrency)
        this.availableConcurrency.set(availableConcurrency)
        this.inFlightMessages.set(inFlight)
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

    override fun recordMessageAck(status: String) {
        registry.counter("redis_stream_consumer_ack_status_total", tags.and("status", status)).increment()
        if (status == "SUCCESS") {
            recordMessageAck()
        }
    }
}
