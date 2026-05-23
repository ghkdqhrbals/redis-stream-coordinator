package com.redisstream.producer

import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.RedisStreamCommands
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamRecords
import java.time.Clock
import java.time.Duration
import java.time.Instant

data class PublishedRedisStreamMessage(
    val streamKey: String,
    val recordId: String,
    val route: ProducerRoute,
)

data class RedisStreamPublishRequest(
    val partitionKey: String,
    val fields: Map<String, String>,
)

interface RedisStreamPublisher {
    fun publish(partitionKey: String, fields: Map<String, String>): PublishedRedisStreamMessage

    fun publish(partitionKey: String, payload: String): PublishedRedisStreamMessage =
        publish(partitionKey, mapOf("payload" to payload))

    fun publishAll(records: Iterable<RedisStreamPublishRequest>): List<PublishedRedisStreamMessage> =
        records.map { publish(it.partitionKey, it.fields) }
}

class RoutingRedisStreamPublisher(
    private val routingCache: ProducerRoutingCache,
    private val writer: RedisStreamWriter,
    private val metrics: RedisStreamProducerMetrics = NoopRedisStreamProducerMetrics,
    private val clock: Clock = Clock.systemUTC(),
) : RedisStreamPublisher {
    override fun publish(partitionKey: String, fields: Map<String, String>): PublishedRedisStreamMessage {
        require(partitionKey.isNotBlank()) { "partitionKey must not be blank" }
        require(fields.isNotEmpty()) { "Redis Stream message fields must not be empty" }

        val startedAt = Instant.now(clock)
        try {
            val route = routingCache.route(partitionKey)
            val recordId = writer.add(route.streamKey, fields)
            metrics.recordPublish("SUCCESS", Duration.between(startedAt, Instant.now(clock)))
            return PublishedRedisStreamMessage(
                streamKey = route.streamKey,
                recordId = recordId,
                route = route,
            )
        } catch (error: RuntimeException) {
            metrics.recordPublish("ERROR", Duration.between(startedAt, Instant.now(clock)))
            throw error
        }
    }
}

interface RedisStreamWriter {
    fun add(streamKey: String, fields: Map<String, String>): String
}

class SpringDataRedisStreamWriter(
    private val redisConnectionFactory: RedisConnectionFactory,
) : RedisStreamWriter {
    override fun add(streamKey: String, fields: Map<String, String>): String {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(fields.isNotEmpty()) { "Redis Stream message fields must not be empty" }

        redisConnectionFactory.connection.use { connection ->
            val record = StreamRecords.newRecord()
                .`in`(streamKey.bytes())
                .ofBytes(fields.mapKeys { it.key.bytes() }.mapValues { it.value.bytes() })
            return connection.streamCommands().xAdd(record, RedisStreamCommands.XAddOptions.none())
                .requireRecordId(streamKey)
        }
    }

    private fun RecordId?.requireRecordId(streamKey: String): String =
        this?.value ?: error("Redis XADD returned no record id for $streamKey")

    private fun String.bytes(): ByteArray =
        toByteArray(Charsets.UTF_8)
}
