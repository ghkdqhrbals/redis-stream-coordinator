package com.redisstream.samples.consumerpod

import com.redisstream.producer.RedisStreamPublisher
import org.springframework.beans.factory.ObjectProvider
import org.springframework.core.env.Environment
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

data class ConsumerPodStatusResponse(
    val memberId: String,
    val listeners: List<ConsumerPodListenerStatus>,
    val eventCount: Int,
)

data class ConsumerPodListenerStatus(
    val id: String,
    val streamPrefix: String,
    val consumerGroupName: String,
    val enabled: Boolean,
)

data class PublishSampleRequest(
    val partitionKey: String,
    val payload: String? = null,
    val fields: Map<String, String> = emptyMap(),
)

data class PublishSampleResponse(
    val streamKey: String,
    val recordId: String,
    val shardIndex: Int,
)

@RestController
@RequestMapping("/sample")
class SampleConsumerPodController(
    environment: Environment,
    private val eventLog: ConsumerPodEventLog,
    private val publisher: ObjectProvider<RedisStreamPublisher>,
) {
    private val memberId = defaultSampleMemberId()
    private val listeners = listOf(
        ConsumerPodListenerStatus(
            id = "consumer-pod-listener",
            streamPrefix = environment.string("STREAM_PREFIX", "create-order"),
            consumerGroupName = environment.string(
                "CONSUMER_GROUP_NAME",
                environment.string("CONSUMER_GROUP", "demo-workers"),
            ),
            enabled = true,
        ),
        ConsumerPodListenerStatus(
            id = "consumer-pod-secondary-listener",
            streamPrefix = environment.string("SECONDARY_STREAM_PREFIX", "create-payment"),
            consumerGroupName = environment.string("SECONDARY_CONSUMER_GROUP_NAME", "payment-workers"),
            enabled = environment.boolean("SECONDARY_STREAM_ENABLED", false),
        ),
        ConsumerPodListenerStatus(
            id = "consumer-pod-payment-low-listener",
            streamPrefix = environment.string("PAYMENT_LOW_STREAM_PREFIX", "create-payment"),
            consumerGroupName = environment.string("PAYMENT_LOW_CONSUMER_GROUP_NAME", "payment-low-workers"),
            enabled = environment.boolean("PAYMENT_LOW_STREAM_ENABLED", false),
        ),
    )

    @GetMapping("/status")
    fun status(): ConsumerPodStatusResponse =
        ConsumerPodStatusResponse(
            memberId = memberId,
            listeners = listeners,
            eventCount = eventLog.snapshot().size,
        )

    @GetMapping("/events")
    fun events(): List<ConsumerPodEvent> =
        eventLog.snapshot()

    @DeleteMapping("/events")
    fun clearEvents() {
        eventLog.clear()
    }

    @PostMapping("/publish")
    fun publish(@RequestBody request: PublishSampleRequest): PublishSampleResponse {
        val fields = request.fields.ifEmpty {
            mapOf("payload" to (request.payload ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST, "payload or fields is required")))
        }
        val message = (publisher.ifAvailable ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "RedisStreamPublisher is not configured"))
            .publish(request.partitionKey, fields)
        return PublishSampleResponse(
            streamKey = message.streamKey,
            recordId = message.recordId,
            shardIndex = message.route.shard.shardIndex,
        )
    }
}

private fun Environment.string(name: String, defaultValue: String): String =
    getProperty(name)?.takeIf { it.isNotBlank() } ?: defaultValue

private fun Environment.boolean(name: String, defaultValue: Boolean): Boolean =
    getProperty(name)?.toBooleanStrictOrNull() ?: defaultValue

private fun defaultSampleMemberId(): String =
    listOfNotNull(
        System.getenv("POD_IP"),
        System.getenv("HOSTNAME"),
    )
        .firstOrNull { it.isNotBlank() }
        ?.replace(Regex("[^A-Za-z0-9_.-]"), "-")
        ?: "consumer-pod"
