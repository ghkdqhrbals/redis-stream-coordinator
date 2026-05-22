package io.github.ghkdqhrbals.redisstreamcoordinator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import java.time.Duration
import java.time.Clock

@SpringBootApplication
@EnableConfigurationProperties(CoordinatorProperties::class)
class RedisStreamCoordinatorApplication {
    @Bean
    fun clock(): Clock = Clock.systemUTC()
}

fun main(args: Array<String>) {
    runApplication<RedisStreamCoordinatorApplication>(*args)
}

@ConfigurationProperties("coordinator")
data class CoordinatorProperties(
    val id: String = "local-coordinator",
    val heartbeatInterval: Duration = Duration.ofSeconds(5),
    val memberLeaseTtl: Duration = Duration.ofSeconds(15),
    val api: Api = Api(),
    val redisCluster: RedisCluster = RedisCluster(),
    val store: Store = Store(),
    val streams: Streams = Streams(),
    val defaults: Defaults = Defaults(),
) {
    data class Api(
        val adminUsername: String = "admin",
        val adminPassword: String = "password",
        val authenticateMemberApi: Boolean = false,
    )

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

    enum class StoreType {
        MEMORY,
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
