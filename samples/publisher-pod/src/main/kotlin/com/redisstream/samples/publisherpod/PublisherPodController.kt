package com.redisstream.samples.publisherpod

import com.redisstream.producer.RedisStreamPublisher
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.http.HttpStatus
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.CrossOrigin
import org.springframework.web.server.ResponseStatusException
import java.time.Instant
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

@ConfigurationProperties("sample.publisher")
class PublisherPodProperties {
    var autoPublishEnabled: Boolean = true
    var partitionKeyPrefix: String = "order"
    var payloadPrefix: String = "sample-message"
}

data class PublisherPodStatusResponse(
    val autoPublishEnabled: Boolean,
    val publishedCount: Long,
    val failedCount: Long,
    val lastEvent: PublisherPodEvent?,
)

data class PublisherPodEvent(
    val type: String,
    val partitionKey: String,
    val payload: String,
    val streamKey: String? = null,
    val recordId: String? = null,
    val shardIndex: Int? = null,
    val error: String? = null,
    val recordedAt: Instant = Instant.now(),
)

data class PublishRequest(
    val partitionKey: String,
    val payload: String? = null,
    val fields: Map<String, String> = emptyMap(),
)

data class PublishResponse(
    val streamKey: String,
    val recordId: String,
    val shardIndex: Int,
)

data class PublishStressRequest(
    val count: Int = 1000,
    val parallelism: Int = 8,
    val partitionKeyPrefix: String? = null,
    val payloadPrefix: String? = null,
)

data class PublishStressResponse(
    val requestedCount: Int,
    val parallelism: Int,
    val publishedCount: Long,
    val failedCount: Long,
    val elapsedMs: Long,
    val publishedPerSecond: Double,
)

@Component
class PublisherPodState {
    private val published = AtomicLong()
    private val failed = AtomicLong()

    @Volatile
    private var lastEvent: PublisherPodEvent? = null

    fun recordPublished(event: PublisherPodEvent) {
        published.incrementAndGet()
        lastEvent = event
    }

    fun recordFailed(event: PublisherPodEvent) {
        failed.incrementAndGet()
        lastEvent = event
    }

    fun status(autoPublishEnabled: Boolean): PublisherPodStatusResponse =
        PublisherPodStatusResponse(
            autoPublishEnabled = autoPublishEnabled,
            publishedCount = published.get(),
            failedCount = failed.get(),
            lastEvent = lastEvent,
        )
}

@Component
class ScheduledSamplePublisher(
    private val properties: PublisherPodProperties,
    private val publisher: RedisStreamPublisher,
    private val state: PublisherPodState,
) {
    private val sequence = AtomicLong()

    @Scheduled(fixedDelayString = "\${sample.publisher.publish-interval-ms:1000}")
    fun publishOnce() {
        if (!properties.autoPublishEnabled) {
            return
        }

        val next = sequence.incrementAndGet()
        val partitionKey = "${properties.partitionKeyPrefix}-$next"
        val payload = "${properties.payloadPrefix}-$next"
        runCatching {
            publisher.publish(partitionKey, payload)
        }.onSuccess { message ->
            state.recordPublished(
                PublisherPodEvent(
                    type = "published",
                    partitionKey = partitionKey,
                    payload = payload,
                    streamKey = message.streamKey,
                    recordId = message.recordId,
                    shardIndex = message.route.shard.shardIndex,
                ),
            )
        }.onFailure { error ->
            state.recordFailed(
                PublisherPodEvent(
                    type = "publish-failed",
                    partitionKey = partitionKey,
                    payload = payload,
                    error = error.message ?: error.javaClass.name,
                ),
            )
        }
    }
}

@RestController
@CrossOrigin
@RequestMapping("/sample")
class PublisherPodController(
    private val properties: PublisherPodProperties,
    private val publisher: RedisStreamPublisher,
    private val state: PublisherPodState,
) {
    @GetMapping("/status")
    fun status(): PublisherPodStatusResponse =
        state.status(properties.autoPublishEnabled)

    @PostMapping("/publish")
    fun publish(@RequestBody request: PublishRequest): PublishResponse {
        val fields = request.fields.ifEmpty {
            mapOf("payload" to (request.payload ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST, "payload or fields is required")))
        }
        val message = publisher.publish(request.partitionKey, fields)
        return PublishResponse(
            streamKey = message.streamKey,
            recordId = message.recordId,
            shardIndex = message.route.shard.shardIndex,
        )
    }

    @PostMapping("/stress")
    fun stress(@RequestBody request: PublishStressRequest): PublishStressResponse {
        val count = request.count.coerceIn(1, 100_000)
        val parallelism = request.parallelism.coerceIn(1, 256)
        val partitionPrefix = request.partitionKeyPrefix?.takeIf { it.isNotBlank() } ?: properties.partitionKeyPrefix
        val payloadPrefix = request.payloadPrefix?.takeIf { it.isNotBlank() } ?: properties.payloadPrefix
        val startedAt = Instant.now()
        val published = AtomicLong()
        val failed = AtomicLong()

        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            val tasks = (1..count).map { index ->
                Callable {
                    val partitionKey = "$partitionPrefix-stress-$index-${System.nanoTime()}"
                    val payload = "$payloadPrefix-stress-$index"
                    runCatching {
                        publisher.publish(partitionKey, payload)
                    }.onSuccess { message ->
                        published.incrementAndGet()
                        state.recordPublished(
                            PublisherPodEvent(
                                type = "stress-published",
                                partitionKey = partitionKey,
                                payload = payload,
                                streamKey = message.streamKey,
                                recordId = message.recordId,
                                shardIndex = message.route.shard.shardIndex,
                            ),
                        )
                    }.onFailure { error ->
                        failed.incrementAndGet()
                        state.recordFailed(
                            PublisherPodEvent(
                                type = "stress-publish-failed",
                                partitionKey = partitionKey,
                                payload = payload,
                                error = error.message ?: error.javaClass.name,
                            ),
                        )
                    }
                }
            }
            tasks.chunked(parallelism).forEach { batch ->
                executor.invokeAll(batch).forEach { it.get() }
            }
        }

        val elapsedMs = Duration.between(startedAt, Instant.now()).toMillis().coerceAtLeast(1)
        val perSecond = published.get().toDouble() * 1000.0 / elapsedMs.toDouble()
        return PublishStressResponse(
            requestedCount = count,
            parallelism = parallelism,
            publishedCount = published.get(),
            failedCount = failed.get(),
            elapsedMs = elapsedMs,
            publishedPerSecond = perSecond,
        )
    }
}
