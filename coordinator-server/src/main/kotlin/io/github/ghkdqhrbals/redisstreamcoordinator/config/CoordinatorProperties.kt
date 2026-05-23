package io.github.ghkdqhrbals.redisstreamcoordinator.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("coordinator")
data class CoordinatorProperties(
    val id: String = "local-coordinator",
    val heartbeatInterval: Duration = Duration.ofSeconds(5),
    val memberLeaseTtl: Duration = Duration.ofSeconds(15),
    val api: Api = Api(),
    val protocol: Protocol = Protocol(),
    val redisCluster: RedisCluster = RedisCluster(),
    val store: Store = Store(),
    val streams: Streams = Streams(),
    val audit: Audit = Audit(),
    val defaults: Defaults = Defaults(),
) {
    data class Api(
        val adminUsername: String = "admin",
        val adminPassword: String = "password",
        val authenticateMemberApi: Boolean = false,
        val users: List<ApiUser> = emptyList(),
    )

    data class ApiUser(
        val username: String = "",
        val password: String = "",
        val roles: Set<ApiRole> = emptySet(),
    )

    enum class ApiRole {
        ADMIN,
        MONITOR,
        MEMBER,
    }

    data class Protocol(
        val minHeartbeatVersion: Int = 1,
        val maxHeartbeatVersion: Int = 1,
    ) {
        fun supportsHeartbeat(version: Int): Boolean =
            version in minHeartbeatVersion..maxHeartbeatVersion
    }

    data class RedisCluster(
        val nodeMappings: List<NodeMapping> = emptyList(),
    )

    data class Store(
        val type: StoreType = StoreType.MEMORY,
        val keyPrefix: String = "redis-stream:coord",
    )

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
