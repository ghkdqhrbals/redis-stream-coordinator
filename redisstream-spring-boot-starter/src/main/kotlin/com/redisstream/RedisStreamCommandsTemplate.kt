package com.redisstream

import org.springframework.data.redis.connection.RedisConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.RedisStreamCommands
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.StreamReadOptions
import org.springframework.data.redis.connection.stream.StreamRecords
import java.time.Duration

data class RedisStreamTemplateRecord(
    val id: String,
    val fields: Map<String, String>,
)

/**
 * Centralizes Redis Stream commands used by producer and consumer adapters.
 */
class RedisStreamCommandsTemplate(
    private val redisConnectionFactory: RedisConnectionFactory,
) {
    /**
     * Executes XREADGROUP and returns decoded stream records.
     */
    fun xReadGroup(
        streamKey: String,
        consumerGroup: String,
        consumerName: String,
        count: Long,
        block: Duration,
    ): List<RedisStreamTemplateRecord> {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(consumerGroup.isNotBlank()) { "consumerGroup must not be blank" }
        require(consumerName.isNotBlank()) { "consumerName must not be blank" }
        require(count > 0) { "count must be positive" }

        return withConnection { connection ->
            connection.streamCommands().xReadGroup(
                Consumer.from(consumerGroup, consumerName),
                StreamReadOptions.empty().count(count).block(block),
                StreamOffset.create(streamKey.bytes(), ReadOffset.lastConsumed()),
            ).orEmpty().map { record ->
                RedisStreamTemplateRecord(
                    id = record.id.value,
                    fields = record.value.mapKeys { it.key.string() }.mapValues { it.value.string() },
                )
            }
        }
    }

    /**
     * Executes XACK for one Redis Stream record.
     */
    fun xAck(streamKey: String, consumerGroup: String, recordId: String) {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(consumerGroup.isNotBlank()) { "consumerGroup must not be blank" }
        require(recordId.isNotBlank()) { "recordId must not be blank" }

        withConnection { connection ->
            connection.streamCommands().xAck(streamKey.bytes(), consumerGroup, RecordId.of(recordId))
        }
    }

    /**
     * Executes XACKDEL for one Redis Stream record using the configured reference policy.
     */
    fun xAckDel(streamKey: String, consumerGroup: String, referencePolicy: String, recordId: String) {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(consumerGroup.isNotBlank()) { "consumerGroup must not be blank" }
        require(referencePolicy.isNotBlank()) { "referencePolicy must not be blank" }
        require(recordId.isNotBlank()) { "recordId must not be blank" }

        withConnection { connection ->
            connection.execute(
                "XACKDEL",
                streamKey.bytes(),
                consumerGroup.bytes(),
                referencePolicy.bytes(),
                "IDS".bytes(),
                "1".bytes(),
                recordId.bytes(),
            )
        }
    }

    /**
     * Executes XNACK for one failed Redis Stream record.
     */
    fun xNack(
        streamKey: String,
        consumerGroup: String,
        mode: String,
        recordId: String,
        retryCount: Long?,
        force: Boolean,
    ) {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(consumerGroup.isNotBlank()) { "consumerGroup must not be blank" }
        require(mode.isNotBlank()) { "mode must not be blank" }
        require(recordId.isNotBlank()) { "recordId must not be blank" }

        withConnection { connection ->
            val args = mutableListOf(
                streamKey.bytes(),
                consumerGroup.bytes(),
                mode.bytes(),
                "IDS".bytes(),
                "1".bytes(),
                recordId.bytes(),
            )
            retryCount?.let {
                require(it >= 0) { "Redis Stream XNACK retryCount must be non-negative" }
                args += "RETRYCOUNT".bytes()
                args += it.toString().bytes()
            }
            if (force) {
                args += "FORCE".bytes()
            }
            connection.execute("XNACK", *args.toTypedArray())
        }
    }

    /**
     * Executes XADD with MAXLEN trimming and returns the generated Redis Stream record id.
     */
    fun xAdd(
        streamKey: String,
        fields: Map<String, String>,
        maxLen: Long,
        approximateTrimming: Boolean,
    ): String {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(fields.isNotEmpty()) { "Redis Stream message fields must not be empty" }
        require(maxLen > 0) { "Redis Stream XADD maxLen must be positive" }

        return withConnection { connection ->
            val record = StreamRecords.newRecord()
                .`in`(streamKey.bytes())
                .ofBytes(fields.mapKeys { it.key.bytes() }.mapValues { it.value.bytes() })
            val options = RedisStreamCommands.XAddOptions.maxlen(maxLen)
                .approximateTrimming(approximateTrimming)
            connection.streamCommands().xAdd(record, options)
                ?.value
                ?: error("Redis XADD returned no record id for $streamKey")
        }
    }

    /**
     * Reads the Redis or Valkey server version used for command compatibility checks.
     */
    fun serverVersion(): String? =
        withConnection { connection ->
            loadVersionFromServerCommands(connection)
                ?: loadVersionFromInfoCommand(connection)
        }

    private fun loadVersionFromServerCommands(connection: RedisConnection): String? =
        runCatching {
            val serverInfo = connection.serverCommands().info("server")
            serverInfo.getProperty("redis_version") ?: serverInfo.getProperty("valkey_version")
        }.getOrNull()

    private fun loadVersionFromInfoCommand(connection: RedisConnection): String? =
        runCatching {
            parseInfoResponse(connection.execute("INFO", "server".bytes()))
        }.getOrNull()

    private fun parseInfoResponse(response: Any?): String? =
        when (response) {
            is ByteArray -> parseInfoText(response.string())
            is String -> parseInfoText(response)
            is Iterable<*> -> response.asSequence().mapNotNull(::parseInfoResponse).firstOrNull()
            else -> null
        }

    private fun parseInfoText(text: String): String? =
        text.lineSequence()
            .map { it.trim() }
            .firstNotNullOfOrNull { line ->
                when {
                    line.startsWith("redis_version:") -> line.substringAfter(":").trim()
                    line.startsWith("valkey_version:") -> line.substringAfter(":").trim()
                    else -> null
                }
            }

    private fun <T> withConnection(block: (RedisConnection) -> T): T =
        redisConnectionFactory.connection.use(block)

    private fun String.bytes(): ByteArray =
        toByteArray(Charsets.UTF_8)

    private fun ByteArray.string(): String =
        toString(Charsets.UTF_8)
}
