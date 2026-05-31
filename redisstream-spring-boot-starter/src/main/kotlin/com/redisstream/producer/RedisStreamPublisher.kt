package com.redisstream.producer

import com.redisstream.RedisStreamCommandsTemplate
import org.springframework.data.redis.connection.RedisConnectionFactory

data class PublishedRedisStreamMessage(
    val streamKey: String,
    val recordId: String,
    val route: ProducerRoute,
)

data class RedisStreamPublishRequest(
    val partitionKey: String,
    val fields: Map<String, String>,
    val options: RedisStreamPublishOptions = RedisStreamPublishOptions(),
)

data class RedisStreamPublishOptions(
    val maxLen: Long? = null,
    val approximateTrimming: Boolean? = null,
)

interface RedisStreamPublisher {
    /**
     * Routes a message by partition key and appends the supplied fields to the selected stream.
     */
    fun publish(
        partitionKey: String,
        fields: Map<String, String>,
        options: RedisStreamPublishOptions,
    ): PublishedRedisStreamMessage

    /**
     * Publishes a field map using the globally configured XADD options.
     */
    fun publish(partitionKey: String, fields: Map<String, String>): PublishedRedisStreamMessage =
        publish(partitionKey, fields, RedisStreamPublishOptions())

    /**
     * Publishes a text payload under the conventional "payload" stream field.
     */
    fun publish(partitionKey: String, payload: String): PublishedRedisStreamMessage =
        publish(partitionKey, mapOf("payload" to payload))

    /**
     * Publishes a text payload with per-message XADD options such as MAXLEN.
     */
    fun publish(
        partitionKey: String,
        payload: String,
        options: RedisStreamPublishOptions,
    ): PublishedRedisStreamMessage =
        publish(partitionKey, mapOf("payload" to payload), options)

    /**
     * Publishes records in caller order while preserving each request's XADD options.
     */
    fun publishAll(records: Iterable<RedisStreamPublishRequest>): List<PublishedRedisStreamMessage> =
        records.map { publish(it.partitionKey, it.fields, it.options) }
}

class RoutingRedisStreamPublisher(
    private val routingCache: ProducerRoutingCache,
    private val writer: RedisStreamWriter,
    private val maxAttempts: Int = 1,
) : RedisStreamPublisher {
    /**
     * Performs route lookup, Redis append, and cache invalidation on failures.
     */
    override fun publish(
        partitionKey: String,
        fields: Map<String, String>,
        options: RedisStreamPublishOptions,
    ): PublishedRedisStreamMessage {
        require(partitionKey.isNotBlank()) { "partitionKey must not be blank" }
        require(fields.isNotEmpty()) { "Redis Stream message fields must not be empty" }
        require(maxAttempts > 0) { "maxAttempts must be positive" }

        var lastError: RuntimeException? = null
        repeat(maxAttempts) {
            val route = routingCache.route(partitionKey)
            try {
                val recordId = writer.add(route.streamKey, fields, options)
                return PublishedRedisStreamMessage(
                    streamKey = route.streamKey,
                    recordId = recordId,
                    route = route,
                )
            } catch (error: RuntimeException) {
                lastError = error
                routingCache.invalidate("write_failure")
            }
        }
        throw lastError ?: IllegalStateException("Redis Stream publish failed")
    }
}

interface RedisStreamWriter {
    /**
     * Appends fields to a concrete Redis Stream key and returns the generated record id.
     */
    fun add(streamKey: String, fields: Map<String, String>): String

    /**
     * Appends fields with per-message XADD options.
     */
    fun add(streamKey: String, fields: Map<String, String>, options: RedisStreamPublishOptions): String =
        add(streamKey, fields)
}

data class RedisStreamXAddConfiguration(
    val maxLen: Long = 10_000_000,
    val approximateTrimming: Boolean = true,
)

class SpringDataRedisStreamWriter(
    private val commands: RedisStreamCommandsTemplate,
    private val xadd: RedisStreamXAddConfiguration = RedisStreamXAddConfiguration(),
) : RedisStreamWriter {
    constructor(
        redisConnectionFactory: RedisConnectionFactory,
        xadd: RedisStreamXAddConfiguration = RedisStreamXAddConfiguration(),
    ) : this(RedisStreamCommandsTemplate(redisConnectionFactory), xadd)

    override fun add(streamKey: String, fields: Map<String, String>): String =
        add(streamKey, fields, RedisStreamPublishOptions())

    /**
     * Executes XADD with configured MAXLEN trimming so stream keys do not grow unbounded.
     */
    override fun add(streamKey: String, fields: Map<String, String>, options: RedisStreamPublishOptions): String {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(fields.isNotEmpty()) { "Redis Stream message fields must not be empty" }
        val maxLen = options.maxLen ?: xadd.maxLen
        val approximateTrimming = options.approximateTrimming ?: xadd.approximateTrimming
        require(maxLen > 0) { "Redis Stream XADD maxLen must be positive" }

        return commands.xAdd(streamKey, fields, maxLen, approximateTrimming)
    }
}
