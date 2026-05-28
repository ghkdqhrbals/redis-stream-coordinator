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
import org.springframework.web.server.ResponseStatusException
import java.time.Instant
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
    val streamVersion: Int? = null,
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
    val streamVersion: Int,
    val shardIndex: Int,
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
                    streamVersion = message.route.shard.streamVersion,
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
            streamVersion = message.route.shard.streamVersion,
            shardIndex = message.route.shard.shardIndex,
        )
    }
}
