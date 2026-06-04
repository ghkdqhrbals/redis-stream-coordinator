package com.redisstream.consumer

import java.net.InetAddress

internal object ConsumerRuntimeIdentity {
    private val podIpEnvironmentKeys = listOf(
        "POD_IP",
        "MY_POD_IP",
        "KUBERNETES_POD_IP",
        "STATUS_POD_IP",
    )
    private val hostnameEnvironmentKeys = listOf(
        "HOSTNAME",
        "COMPUTERNAME",
    )

    /**
     * Builds the base coordinator member id from the runtime context.
     *
     * Kubernetes users should expose `status.podIP` as `POD_IP` through the Downward API.
     * Local and Docker runtimes fall back to the process host address or hostname.
     */
    fun defaultMemberId(): String =
        defaultMemberId(
            environment = System.getenv(),
            localAddress = { localHostAddress() },
        )

    internal fun defaultMemberId(
        environment: Map<String, String>,
        localAddress: () -> String?,
    ): String {
        val rawIdentity = firstNonBlank(environment, podIpEnvironmentKeys)
            ?: localAddress().takeUnless { it.isNullOrBlank() }
            ?: firstNonBlank(environment, hostnameEnvironmentKeys)
            ?: "redis-stream-consumer"
        return sanitize(rawIdentity)
    }

    private fun firstNonBlank(environment: Map<String, String>, keys: List<String>): String? =
        keys.asSequence()
            .mapNotNull(environment::get)
            .map(String::trim)
            .firstOrNull(String::isNotBlank)

    private fun localHostAddress(): String? =
        runCatching { InetAddress.getLocalHost().hostAddress }.getOrNull()

    private fun sanitize(value: String): String {
        val sanitized = value.trim()
            .replace(Regex("[^A-Za-z0-9._-]"), "-")
            .trim('-', '.', '_')
        return sanitized.ifBlank { "redis-stream-consumer" }
    }
}
