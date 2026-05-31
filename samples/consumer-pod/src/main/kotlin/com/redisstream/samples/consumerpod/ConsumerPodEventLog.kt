package com.redisstream.samples.consumerpod

import com.redisstream.consumer.ConsumedRedisStreamMessage
import com.redisstream.consumer.CoordinatorConsumerContext
import com.redisstream.consumer.CoordinatorShard
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.ArrayDeque

data class ConsumerPodEvent(
    val at: Instant,
    val type: String,
    val memberId: String,
    val consumerGroupName: String,
    val groupEpoch: Long,
    val assignmentEpoch: Long,
    val metadataVersion: Long,
    val shards: List<String> = emptyList(),
    val streamKey: String? = null,
    val recordId: String? = null,
    val fields: Map<String, String> = emptyMap(),
)

@Component
class ConsumerPodEventLog {
    private val events = ArrayDeque<ConsumerPodEvent>()

    @Synchronized
    fun record(type: String, shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        append(
            ConsumerPodEvent(
                at = Instant.now(),
                type = type,
                memberId = context.memberId,
                consumerGroupName = context.memberName,
                groupEpoch = context.groupEpoch,
                assignmentEpoch = context.assignmentEpoch,
                metadataVersion = context.metadataVersion,
                shards = shards.sorted().map { it.label() },
            ),
        )
    }

    @Synchronized
    fun record(type: String, message: ConsumedRedisStreamMessage, context: CoordinatorConsumerContext) {
        append(
            ConsumerPodEvent(
                at = Instant.now(),
                type = type,
                memberId = context.memberId,
                consumerGroupName = context.memberName,
                groupEpoch = context.groupEpoch,
                assignmentEpoch = context.assignmentEpoch,
                metadataVersion = context.metadataVersion,
                shards = listOf(message.shard.label()),
                streamKey = message.streamKey,
                recordId = message.recordId,
                fields = message.fields,
            ),
        )
    }

    @Synchronized
    fun snapshot(): List<ConsumerPodEvent> =
        events.toList()

    @Synchronized
    fun clear() {
        events.clear()
    }

    private fun append(event: ConsumerPodEvent) {
        events.addLast(event)
        while (events.size > MAX_EVENTS) {
            events.removeFirst()
        }
    }

    private fun CoordinatorShard.label(): String =
        "v$streamVersion:$shardIndex"

    private companion object {
        const val MAX_EVENTS = 200
    }
}
