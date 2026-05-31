package com.redisstream.consumer

import com.redisstream.RedisStreamCommandsTemplate
import org.springframework.data.redis.connection.RedisConnectionFactory

data class RedisServerVersion(
    val major: Int,
    val minor: Int,
    val patch: Int,
    val raw: String,
) : Comparable<RedisServerVersion> {
    override fun compareTo(other: RedisServerVersion): Int =
        compareValuesBy(this, other, RedisServerVersion::major, RedisServerVersion::minor, RedisServerVersion::patch)

    companion object {
        /**
         * Parses Redis or Valkey semantic version text from INFO output.
         */
        fun parse(raw: String): RedisServerVersion {
            val parts = raw.trim()
                .substringBefore("-")
                .split(".")
                .map { it.takeWhile(Char::isDigit).toIntOrNull() ?: 0 }
            return RedisServerVersion(
                major = parts.getOrElse(0) { 0 },
                minor = parts.getOrElse(1) { 0 },
                patch = parts.getOrElse(2) { 0 },
                raw = raw,
            )
        }
    }
}

data class RedisStreamCommandSupport(
    val serverVersion: RedisServerVersion,
    val supportsXAckDel: Boolean,
    val supportsXNack: Boolean,
) {
    companion object {
        /**
         * Converts a server version into the Redis Stream command feature matrix used by the starter.
         */
        fun fromVersion(version: RedisServerVersion): RedisStreamCommandSupport =
            RedisStreamCommandSupport(
                serverVersion = version,
                supportsXAckDel = version >= RedisServerVersion(8, 2, 0, "8.2.0"),
                supportsXNack = version >= RedisServerVersion(8, 8, 0, "8.8.0"),
            )
    }
}

fun interface RedisStreamCommandSupportProvider {
    /**
     * Returns the command support for the currently connected Redis server.
     */
    fun current(): RedisStreamCommandSupport
}

class RedisConnectionStreamCommandSupportProvider(
    private val commands: RedisStreamCommandsTemplate,
) : RedisStreamCommandSupportProvider {
    constructor(redisConnectionFactory: RedisConnectionFactory) : this(RedisStreamCommandsTemplate(redisConnectionFactory))

    @Volatile
    private var cached: RedisStreamCommandSupport? = null

    /**
     * Loads Redis server command support once and reuses it for later ack/nack decisions.
     */
    override fun current(): RedisStreamCommandSupport =
        cached ?: synchronized(this) {
            cached ?: load().also { cached = it }
        }

    private fun load(): RedisStreamCommandSupport {
        val rawVersion = commands.serverVersion()
            ?: error("Redis INFO server response did not include redis_version or valkey_version")
        return RedisStreamCommandSupport.fromVersion(RedisServerVersion.parse(rawVersion))
    }
}

object RedisStreamCommandCompatibility {
    /**
     * Resolves AUTO acknowledgement mode and rejects XACKDEL when the server is too old.
     */
    fun resolveAckMode(
        requested: RedisStreamAckMode,
        support: RedisStreamCommandSupport,
    ): RedisStreamResolvedAckMode =
        when (requested) {
            RedisStreamAckMode.AUTO ->
                if (support.supportsXAckDel) RedisStreamResolvedAckMode.XACKDEL else RedisStreamResolvedAckMode.XACK
            RedisStreamAckMode.XACK -> RedisStreamResolvedAckMode.XACK
            RedisStreamAckMode.XACKDEL -> {
                require(support.supportsXAckDel) {
                    "Redis ${support.serverVersion.raw} does not support XACKDEL; Redis 8.2.0 or newer is required"
                }
                RedisStreamResolvedAckMode.XACKDEL
            }
        }

    /**
     * Rejects XNACK configuration before the consumer sends an unsupported Redis command.
     */
    fun validateFailureMode(
        requested: RedisStreamFailureMode,
        support: RedisStreamCommandSupport,
    ) {
        if (requested == RedisStreamFailureMode.XNACK) {
            require(support.supportsXNack) {
                "Redis ${support.serverVersion.raw} does not support XNACK; Redis 8.8.0 or newer is required"
            }
        }
    }
}

enum class RedisStreamResolvedAckMode { XACK, XACKDEL }
