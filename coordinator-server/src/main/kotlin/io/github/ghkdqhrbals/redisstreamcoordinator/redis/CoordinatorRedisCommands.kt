package io.github.ghkdqhrbals.redisstreamcoordinator.redis

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.connection.RedisConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.RedisScript
import java.nio.charset.StandardCharsets
import java.time.Duration

/**
 * Centralizes Redis commands issued by the coordinator server.
 */
class CoordinatorRedisCommands(
    private val redisTemplate: StringRedisTemplate? = null,
    private val redisConnectionFactory: RedisConnectionFactory? = null,
) {
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
        withConnection { connection ->
            connection.streamCommands().xGroupCreate(streamKey.bytes(), consumerGroup, ReadOffset.latest(), true)
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
}

@Configuration
class CoordinatorRedisCommandsConfig {
    @Bean
    @ConditionalOnBean(RedisConnectionFactory::class)
    @ConditionalOnMissingBean
    fun coordinatorRedisCommands(
        redisTemplate: org.springframework.beans.factory.ObjectProvider<StringRedisTemplate>,
        redisConnectionFactory: RedisConnectionFactory,
    ): CoordinatorRedisCommands =
        CoordinatorRedisCommands(redisTemplate.ifAvailable, redisConnectionFactory)
}

fun Throwable.isRedisBusyGroup(): Boolean =
    generateSequence(this) { it.cause }
        .any { it.message?.contains("BUSYGROUP", ignoreCase = true) == true }

fun DataAccessException.isRedisBusyGroup(): Boolean =
    (this as Throwable).isRedisBusyGroup()
