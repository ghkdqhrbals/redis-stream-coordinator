package io.github.ghkdqhrbals.redisstreamcoordinator.config

import com.redisstream.protocol.CoordinatorProtocol
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("coordinator")
data class CoordinatorProperties(
    val id: String = "local-coordinator",
    val heartbeatInterval: Duration = CoordinatorProtocol.DEFAULT_TIMING.heartbeatInterval,
    val memberLeaseTtl: Duration = CoordinatorProtocol.DEFAULT_TIMING.memberLeaseTtl,
    val staleMemberRetention: Duration = Duration.ofMinutes(10),
    val rebalanceTimeout: Duration = CoordinatorProtocol.DEFAULT_TIMING.rebalanceTimeout,
    val loop: Loop = Loop(),
    val api: Api = Api(),
    val redisCluster: RedisCluster = RedisCluster(),
    val store: Store = Store(),
    val coordination: Coordination = Coordination(),
    val health: Health = Health(),
    val streams: Streams = Streams(),
    val audit: Audit = Audit(),
    val defaults: Defaults = Defaults(),
) {
    data class Loop(
        val enabled: Boolean = true,
        val tickInterval: Duration = Duration.ofSeconds(1),
    )

    data class Api(
        val adminUsername: String = "admin",
        val adminPassword: String = "password",
        val authenticateMemberApi: Boolean = false,
        val users: List<ApiUser> = emptyList(),
        val rateLimit: RateLimit = RateLimit(),
    )

    data class RateLimit(
        val enabled: Boolean = false,
        val adminMutationsPerMinute: Int = 60,
    )

    data class ApiUser(
        val username: String = "",
        val password: String = "",
        val roles: Set<ApiRole> = emptySet(),
    )

    enum class ApiRole {
        READ,
        WRITE,
        ADMIN,
        MONITOR,
        MEMBER,
    }

    data class RedisCluster(
        val nodeMappings: List<NodeMapping> = emptyList(),
    )

    data class Store(
        val type: StoreType = StoreType.MEMORY,
        val keyPrefix: String = "redis-stream:coord",
    )

    data class Coordination(
        val stateMutex: StateMutex = StateMutex(),
    )

    data class Health(
        val redisTimeoutMs: Long = 300,
    )

    data class StateMutex(
        val enabled: Boolean = true,
        val ttlMs: Long = 30_000,
        val acquireTimeoutMs: Long = 5_000,
        val retryIntervalMs: Long = 100,
        @Deprecated("Use ttl-ms instead.")
        val ttl: Duration? = null,
        @Deprecated("Use acquire-timeout-ms instead.")
        val acquireTimeout: Duration? = null,
        @Deprecated("Use retry-interval-ms instead.")
        val retryInterval: Duration? = null,
    ) {
        @Suppress("DEPRECATION")
        val resolvedTtl: Duration
            get() = ttl ?: Duration.ofMillis(ttlMs)

        @Suppress("DEPRECATION")
        val resolvedAcquireTimeout: Duration
            get() = acquireTimeout ?: Duration.ofMillis(acquireTimeoutMs)

        @Suppress("DEPRECATION")
        val resolvedRetryInterval: Duration
            get() = retryInterval ?: Duration.ofMillis(retryIntervalMs)
    }

    data class Streams(
        val provisioningEnabled: Boolean = false,
    )

    data class Audit(
        val sink: AuditSink = AuditSink.LOG,
        val redisMaxEntries: Long = 1_000,
    )

    enum class StoreType {
        MEMORY,
        REDIS,
        JDBC,
    }

    enum class AuditSink {
        LOG,
        REDIS,
    }

    data class NodeMapping(
        val advertisedHost: String,
        val advertisedPort: Int,
        val connectHost: String,
        val connectPort: Int,
    )

    data class Defaults(
        val initialShardCount: Int = 12,
        val consumerMaxConcurrency: Int = 12,
    )
}
