package io.github.ghkdqhrbals.redisstreamcoordinator.redis

import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.output.IntegerOutput
import io.lettuce.core.protocol.CommandArgs
import io.lettuce.core.protocol.CommandType
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.dao.DataAccessException
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.RedisConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.Limit
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
import java.nio.charset.StandardCharsets
import java.time.Duration

data class RedisStreamInfo(
    val length: Long,
    val firstEntryId: String?,
    val lastEntryId: String?,
    val lastGeneratedId: String?,
    val entriesAdded: Long?,
)

data class RedisStreamGroupInfo(
    val name: String,
    val consumers: Long?,
    val pending: Long?,
    val lastDeliveredId: String?,
    val entriesRead: Long?,
    val lag: Long?,
)

data class RedisStreamRecord(
    val id: String,
    val fields: Map<String, String>,
)

data class RedisClusterSlotOwner(
    val nodeId: String?,
    val endpoint: String,
    val host: String,
    val port: Int,
    val slotRangeStart: Int,
    val slotRangeEnd: Int,
)

/**
 * Centralizes Redis commands issued by the coordinator server.
 */
open class CoordinatorRedisCommands(
    private val redisTemplate: StringRedisTemplate? = null,
    private val redisConnectionFactory: RedisConnectionFactory? = null,
) {
    /**
     * Returns whether both template and connection access are available for Redis-backed features.
     */
    fun isConfigured(): Boolean =
        redisTemplate != null && redisConnectionFactory != null

    /**
     * Reads whether a Redis key exists.
     */
    fun hasKey(key: String): Boolean =
        stringRedisTemplate().hasKey(key)

    /**
     * Reads a string value by key.
     */
    fun getValue(key: String): String? =
        stringRedisTemplate().opsForValue().get(key)

    /**
     * Reads a string field from a Redis hash.
     */
    fun hashGet(key: String, field: String): String? =
        stringRedisTemplate().opsForHash<String, String>().get(key, field)

    /**
     * Adds a member to a Redis set.
     */
    fun setAdd(key: String, value: String) {
        stringRedisTemplate().opsForSet().add(key, value)
    }

    /**
     * Removes a member from a Redis set.
     */
    fun setRemove(key: String, value: String) {
        stringRedisTemplate().opsForSet().remove(key, value)
    }

    /**
     * Reads all members of a Redis set.
     */
    fun setMembers(key: String): Set<String> =
        stringRedisTemplate().opsForSet().members(key).orEmpty()

    /**
     * Executes a Lua script that returns a Long.
     */
    fun executeLong(script: RedisScript<Long>, keys: List<String>, vararg args: String): Long? =
        stringRedisTemplate().execute(script, keys, *args)

    /**
     * Acquires a mutex token with SET NX PX semantics.
     */
    fun setIfAbsent(key: String, value: String, ttl: Duration): Boolean =
        stringRedisTemplate().opsForValue().setIfAbsent(key, value, ttl) == true

    /**
     * Appends an audit entry and trims the list to the newest retained entries.
     */
    fun rightPushAndTrim(key: String, value: String, maxEntries: Long) {
        val retained = maxEntries.coerceAtLeast(1)
        val operations = stringRedisTemplate().opsForList()
        operations.rightPush(key, value)
        operations.trim(key, -retained, -1L)
    }

    /**
     * Pings Redis through the configured connection factory.
     */
    fun ping(): String =
        withConnection { it.ping() }

    /**
     * Creates a Redis Stream consumer group, optionally creating the stream key.
     */
    fun xGroupCreate(streamKey: String, consumerGroup: String) {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(consumerGroup.isNotBlank()) { "consumerGroup must not be blank" }
        val entriesRead = xInfoStream(streamKey).entriesAdded ?: 0L
        withConnection { connection ->
            connection.execute(
                "XGROUP",
                "CREATE".bytes(),
                streamKey.bytes(),
                consumerGroup.bytes(),
                "$".bytes(),
                "MKSTREAM".bytes(),
                "ENTRIESREAD".bytes(),
                entriesRead.toString().bytes(),
            )
        }
    }

    /**
     * Reads Redis Stream length and boundary offsets for monitoring dashboards.
     */
    open fun xInfoStream(streamKey: String): RedisStreamInfo {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        return runCatching {
            val info = withConnection { connection -> connection.streamCommands().xInfo(streamKey.bytes()) }
            RedisStreamInfo(
                length = info.streamLength(),
                firstEntryId = info.firstEntryId(),
                lastEntryId = info.lastEntryId(),
                lastGeneratedId = info.lastGeneratedId(),
                entriesAdded = info.getRaw()["entries-added"].longValue(),
            )
        }.getOrElse {
            RedisStreamInfo(length = 0, firstEntryId = null, lastEntryId = null, lastGeneratedId = null, entriesAdded = null)
        }
    }

    /**
     * Reads Redis Stream consumer group progress, including lag when Redis can calculate it.
     */
    open fun xInfoGroups(streamKey: String): List<RedisStreamGroupInfo> {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        return runCatching {
            withConnection { connection -> connection.streamCommands().xInfoGroups(streamKey.bytes()) }.map { group ->
                RedisStreamGroupInfo(
                    name = group.groupName(),
                    consumers = group.consumerCount(),
                    pending = group.pendingCount(),
                    lastDeliveredId = group.lastDeliveredId(),
                    entriesRead = group.getRaw()["entries-read"].longValue(),
                    lag = group.getRaw()["lag"].longValue(),
                )
            }.toList()
        }.getOrDefault(emptyList())
    }

    /**
     * Reads Redis memory usage in bytes for one stream key.
     */
    open fun memoryUsage(key: String): Long? {
        require(key.isNotBlank()) { "key must not be blank" }
        return runCatching { memoryUsageWithClusterRouting(key) ?: memoryUsageWithRawCommand(key) }
            .getOrNull()
    }

    /**
     * Resolves Redis Cluster master nodes for hash slots. Standalone Redis returns an empty map.
     */
    fun clusterSlotOwners(slots: Collection<Int>): Map<Int, RedisClusterSlotOwner> {
        val requestedSlots = slots.filter { it in 0 until REDIS_CLUSTER_SLOT_COUNT }.distinct()
        if (requestedSlots.isEmpty()) {
            return emptyMap()
        }

        val ranges = runCatching { clusterSlotRangesWithNative() ?: clusterSlotRangesWithRawCommand() }
            .getOrDefault(emptyList())
        if (ranges.isEmpty()) {
            return emptyMap()
        }

        return requestedSlots.mapNotNull { slot ->
            ranges.firstOrNull { slot in it.slotRangeStart..it.slotRangeEnd }?.let { owner -> slot to owner }
        }.toMap()
    }

    private fun memoryUsageWithClusterRouting(key: String): Long? =
        withConnection { connection ->
            val nativeConnection = connection.nativeConnection
            if (nativeConnection is RedisClusterAsyncCommands<*, *>) {
                @Suppress("UNCHECKED_CAST")
                val commands = nativeConnection as RedisClusterAsyncCommands<ByteArray, ByteArray>
                val args = CommandArgs(ByteArrayCodec.INSTANCE)
                    .add("USAGE")
                    .addKey(key.bytes())
                commands.dispatch(CommandType.MEMORY, IntegerOutput(ByteArrayCodec.INSTANCE), args).get()
            } else {
                null
            }
        }

    private fun memoryUsageWithRawCommand(key: String): Long? =
        withConnection { connection ->
            connection.execute("MEMORY", "USAGE".bytes(), key.bytes())
        }.longValue()

    private fun clusterSlotRangesWithNative(): List<RedisClusterSlotOwner>? =
        withConnection { connection ->
            val nativeConnection = connection.nativeConnection
            if (nativeConnection is RedisClusterAsyncCommands<*, *>) {
                @Suppress("UNCHECKED_CAST")
                val commands = nativeConnection as RedisClusterAsyncCommands<ByteArray, ByteArray>
                parseClusterSlots(commands.clusterSlots().get())
            } else {
                null
            }
        }

    private fun clusterSlotRangesWithRawCommand(): List<RedisClusterSlotOwner> =
        parseClusterSlots(
            withConnection { connection ->
                connection.execute("CLUSTER", "SLOTS".bytes())
            },
        )

    /**
     * Reads Redis Stream records in ascending id order.
     */
    open fun xRange(streamKey: String, start: String, end: String, count: Long): List<RedisStreamRecord> {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(count > 0) { "count must be positive" }
        return withConnection { connection ->
            connection.streamCommands()
                .xRange(streamKey.bytes(), Range.closed(start, end), Limit.limit().count(count.toInt()))
                .orEmpty()
                .map { it.toRedisStreamRecord() }
        }
    }

    /**
     * Reads Redis Stream records in descending id order.
     */
    open fun xRevRange(streamKey: String, start: String, end: String, count: Long): List<RedisStreamRecord> {
        require(streamKey.isNotBlank()) { "streamKey must not be blank" }
        require(count > 0) { "count must be positive" }
        return withConnection { connection ->
            connection.streamCommands()
                .xRevRange(streamKey.bytes(), Range.closed(end, start), Limit.limit().count(count.toInt()))
                .orEmpty()
                .map { it.toRedisStreamRecord() }
        }
    }

    private fun stringRedisTemplate(): StringRedisTemplate =
        requireNotNull(redisTemplate) { "StringRedisTemplate is not configured" }

    private fun <T> withConnection(block: (RedisConnection) -> T): T =
        requireNotNull(redisConnectionFactory) { "RedisConnectionFactory is not configured" }
            .connection
            .use(block)

    private fun String.bytes(): ByteArray =
        toByteArray(StandardCharsets.UTF_8)

    private companion object {
        private const val REDIS_CLUSTER_SLOT_COUNT = 16_384
    }
}

private fun parseClusterSlots(response: Any?): List<RedisClusterSlotOwner> =
    response.listItems().mapNotNull { range ->
        val parts = range.listItems()
        val slotRangeStart = parts.getOrNull(0).longValue()?.toInt() ?: return@mapNotNull null
        val slotRangeEnd = parts.getOrNull(1).longValue()?.toInt() ?: return@mapNotNull null
        val master = parts.getOrNull(2).listItems()
        val host = master.getOrNull(0).stringValue()?.takeIf { it.isNotBlank() } ?: return@mapNotNull null
        val port = master.getOrNull(1).longValue()?.toInt() ?: return@mapNotNull null
        val nodeId = master.getOrNull(2).stringValue()?.takeIf { it.isNotBlank() }
        RedisClusterSlotOwner(
            nodeId = nodeId,
            endpoint = "$host:$port",
            host = host,
            port = port,
            slotRangeStart = slotRangeStart,
            slotRangeEnd = slotRangeEnd,
        )
    }

private fun org.springframework.data.redis.connection.stream.ByteRecord.toRedisStreamRecord(): RedisStreamRecord =
    RedisStreamRecord(
        id = id.value,
        fields = value.mapKeys { it.key.stringValue().orEmpty() }.mapValues { it.value.stringValue().orEmpty() },
    )

private fun parseStreamRecords(response: Any?): List<RedisStreamRecord> =
    response.listItems().mapNotNull { entry ->
        val parts = entry.listItems()
        val id = parts.getOrNull(0).stringValue() ?: return@mapNotNull null
        val fieldValues = parts.getOrNull(1).listItems()
        RedisStreamRecord(
            id = id,
            fields = fieldValues.chunked(2).mapNotNull { pair ->
                val key = pair.getOrNull(0).stringValue() ?: return@mapNotNull null
                val value = pair.getOrNull(1).stringValue().orEmpty()
                key to value
            }.toMap(),
        )
    }

private fun parseKeyValueResponse(response: Any?): Map<String, Any?> =
    response.listItems()
        .chunked(2)
        .mapNotNull { pair ->
            val key = pair.getOrNull(0).stringValue() ?: return@mapNotNull null
            key to pair.getOrNull(1)
        }
        .toMap()

private fun Any?.listItems(): List<Any?> =
    when (this) {
        is Iterable<*> -> this.toList()
        is Array<*> -> this.toList()
        else -> emptyList()
    }

private fun Any?.stringValue(): String? =
    when (this) {
        is ByteArray -> toString(StandardCharsets.UTF_8)
        is String -> this
        is Number -> toString()
        else -> null
    }

private fun Any?.longValue(): Long? =
    when (this) {
        is Number -> toLong()
        else -> stringValue()?.toLongOrNull()
    }

private fun Any?.streamEntryId(): String? =
    listItems().firstOrNull().stringValue()

@Configuration
class CoordinatorRedisCommandsConfig {
    @Bean
    @ConditionalOnMissingBean
    fun coordinatorRedisCommands(
        redisTemplate: ObjectProvider<StringRedisTemplate>,
        redisConnectionFactory: ObjectProvider<RedisConnectionFactory>,
    ): CoordinatorRedisCommands =
        CoordinatorRedisCommands(redisTemplate.ifAvailable, redisConnectionFactory.ifAvailable)
}

fun Throwable.isRedisBusyGroup(): Boolean =
    generateSequence(this) { it.cause }
        .any { it.message?.contains("BUSYGROUP", ignoreCase = true) == true }

fun DataAccessException.isRedisBusyGroup(): Boolean =
    (this as Throwable).isRedisBusyGroup()
