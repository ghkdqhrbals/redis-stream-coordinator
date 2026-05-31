package com.redisstream.samples.consumerpod

import com.redisstream.consumer.CoordinatorConsumerProperties
import com.redisstream.producer.RedisStreamPublisher
import org.springframework.beans.factory.ObjectProvider
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
    val streamPrefix: String,
    val consumerGroupName: String,
    val eventCount: Int,
)

data class PublishSampleRequest(
    val partitionKey: String,
    val payload: String? = null,
    val fields: Map<String, String> = emptyMap(),
)

data class PublishSampleResponse(
    val streamKey: String,
    val recordId: String,
    val streamVersion: Int,
    val shardIndex: Int,
)

@RestController
@RequestMapping("/sample")
class SampleConsumerPodController(
    private val properties: CoordinatorConsumerProperties,
    private val eventLog: ConsumerPodEventLog,
    private val publisher: ObjectProvider<RedisStreamPublisher>,
) {
    @GetMapping("/status")
    fun status(): ConsumerPodStatusResponse =
        ConsumerPodStatusResponse(
            memberId = properties.memberId,
            streamPrefix = properties.streamPrefix,
            consumerGroupName = properties.consumerGroupName,
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
            streamVersion = message.route.shard.streamVersion,
            shardIndex = message.route.shard.shardIndex,
        )
    }
}
