package com.redisstream.producer

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import java.time.Duration

interface RedisStreamProducerMetrics {
    fun recordRoutingCacheHit()
    fun recordRoutingCacheInvalidated(reason: String) {
    }
    fun recordRoutingRefresh(status: String, duration: Duration)
    fun recordPublishAttempt(status: String, attempt: Int, duration: Duration) {
    }
    fun recordPublish(status: String, duration: Duration)
}

object NoopRedisStreamProducerMetrics : RedisStreamProducerMetrics {
    override fun recordRoutingCacheHit() = Unit
    override fun recordRoutingCacheInvalidated(reason: String) = Unit
    override fun recordRoutingRefresh(status: String, duration: Duration) = Unit
    override fun recordPublishAttempt(status: String, attempt: Int, duration: Duration) = Unit
    override fun recordPublish(status: String, duration: Duration) = Unit
}

class MicrometerRedisStreamProducerMetrics(
    private val registry: MeterRegistry,
    streamPrefix: String,
    consumerGroup: String,
) : RedisStreamProducerMetrics {
    private val tags = Tags.of(
        "stream", streamPrefix.ifBlank { "unknown" },
        "group", consumerGroup.ifBlank { "unknown" },
    )

    override fun recordRoutingCacheHit() {
        registry.counter("redis_stream_producer_routing_cache_hit_total", tags).increment()
    }

    override fun recordRoutingCacheInvalidated(reason: String) {
        registry.counter("redis_stream_producer_routing_cache_invalidated_total", tags.and("reason", reason)).increment()
    }

    override fun recordRoutingRefresh(status: String, duration: Duration) {
        registry.counter("redis_stream_producer_routing_refresh_total", tags.and("status", status)).increment()
        registry.timer("redis_stream_producer_routing_refresh_duration", tags.and("status", status)).record(duration)
    }

    override fun recordPublish(status: String, duration: Duration) {
        registry.counter("redis_stream_producer_publish_total", tags.and("status", status)).increment()
        registry.timer("redis_stream_producer_publish_duration", tags.and("status", status)).record(duration)
    }

    override fun recordPublishAttempt(status: String, attempt: Int, duration: Duration) {
        val attemptTag = attempt.coerceAtLeast(1).toString()
        registry.counter("redis_stream_producer_publish_attempt_total", tags.and("status", status, "attempt", attemptTag)).increment()
        registry.timer("redis_stream_producer_publish_attempt_duration", tags.and("status", status, "attempt", attemptTag))
            .record(duration)
    }
}
