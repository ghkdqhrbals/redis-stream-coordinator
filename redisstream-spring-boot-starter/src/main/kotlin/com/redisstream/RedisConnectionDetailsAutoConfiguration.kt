package com.redisstream

import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.data.redis.autoconfigure.DataRedisConnectionDetails
import org.springframework.boot.data.redis.autoconfigure.DataRedisProperties
import org.springframework.boot.ssl.SslBundle
import org.springframework.boot.ssl.SslBundles
import org.springframework.context.annotation.Bean
import org.springframework.util.StringUtils
import java.net.URI

@AutoConfiguration(beforeName = ["org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration"])
@ConditionalOnClass(DataRedisConnectionDetails::class, DataRedisProperties::class)
class RedisConnectionDetailsAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(DataRedisConnectionDetails::class)
    fun redisStreamDataRedisConnectionDetails(
        properties: DataRedisProperties,
        sslBundles: ObjectProvider<SslBundles>,
    ): DataRedisConnectionDetails =
        NormalizedDataRedisConnectionDetails(properties, sslBundles.ifAvailable)
}

private class NormalizedDataRedisConnectionDetails(
    private val properties: DataRedisProperties,
    private val sslBundles: SslBundles?,
) : DataRedisConnectionDetails {
    private val redisUri: URI? = properties.url?.takeIf(StringUtils::hasText)?.let(URI::create)
    private val credentials: RedisCredentials? = redisUri?.userInfo?.let(::parseCredentials)

    override fun getUsername(): String? =
        credentials?.username ?: properties.username

    override fun getPassword(): String? =
        credentials?.password ?: properties.password

    override fun getSslBundle(): SslBundle? {
        if (!properties.ssl.isEnabled && redisUri?.scheme != "rediss") {
            return null
        }
        val bundleName = properties.ssl.bundle?.takeIf(StringUtils::hasText)
        return if (bundleName != null) {
            requireNotNull(sslBundles) { "SSL bundle name has been set but no SSL bundles found in context" }
                .getBundle(bundleName)
        } else {
            SslBundle.systemDefault()
        }
    }

    override fun getStandalone(): DataRedisConnectionDetails.Standalone =
        redisUri?.let { uri ->
            DataRedisConnectionDetails.Standalone.of(
                requireNotNull(uri.host) { "Redis URL host must not be null" },
                uri.port.takeIf { it > 0 } ?: DEFAULT_REDIS_PORT,
                uri.database() ?: properties.database,
            )
        } ?: DataRedisConnectionDetails.Standalone.of(properties.host, properties.port, properties.database)

    override fun getSentinel(): DataRedisConnectionDetails.Sentinel? {
        val sentinel = properties.sentinel ?: return null
        val nodes = parseNodes(sentinel.nodes)
        val master = sentinel.master?.takeIf(StringUtils::hasText)
        if (master == null || nodes.isEmpty()) {
            return null
        }
        return object : DataRedisConnectionDetails.Sentinel {
            override fun getDatabase(): Int = standalone.database
            override fun getMaster(): String = master
            override fun getNodes(): List<DataRedisConnectionDetails.Node> = nodes
            override fun getUsername(): String? = sentinel.username
            override fun getPassword(): String? = sentinel.password
        }
    }

    override fun getCluster(): DataRedisConnectionDetails.Cluster? {
        val nodes = parseNodes(properties.cluster?.nodes)
        if (nodes.isEmpty()) {
            return null
        }
        return DataRedisConnectionDetails.Cluster { nodes }
    }

    override fun getMasterReplica(): DataRedisConnectionDetails.MasterReplica? {
        val nodes = parseNodes(properties.masterreplica?.nodes)
        if (nodes.isEmpty()) {
            return null
        }
        return DataRedisConnectionDetails.MasterReplica { nodes }
    }

    private fun URI.database(): Int? =
        path
            ?.trim('/')
            ?.takeIf(StringUtils::hasText)
            ?.toIntOrNull()

    private companion object {
        private const val DEFAULT_REDIS_PORT = 6379
    }
}

private data class RedisCredentials(
    val username: String?,
    val password: String?,
)

private fun parseCredentials(userInfo: String): RedisCredentials {
    val separatorIndex = userInfo.indexOf(':')
    if (separatorIndex < 0) {
        return RedisCredentials(username = userInfo.takeIf(StringUtils::hasText), password = null)
    }
    return RedisCredentials(
        username = userInfo.substring(0, separatorIndex).takeIf(StringUtils::hasText),
        password = userInfo.substring(separatorIndex + 1).takeIf(StringUtils::hasText),
    )
}

private fun parseNodes(nodes: List<String>?): List<DataRedisConnectionDetails.Node> =
    nodes.orEmpty()
        .flatMap { it.split(',') }
        .map { it.trim() }
        .filter(StringUtils::hasText)
        .map(::parseNode)

private fun parseNode(node: String): DataRedisConnectionDetails.Node {
    val portSeparatorIndex = node.lastIndexOf(':')
    require(portSeparatorIndex > 0 && portSeparatorIndex < node.lastIndex - 1) {
        "Redis node must be formatted as host:port"
    }
    return DataRedisConnectionDetails.Node(
        node.substring(0, portSeparatorIndex),
        node.substring(portSeparatorIndex + 1).toInt(),
    )
}
