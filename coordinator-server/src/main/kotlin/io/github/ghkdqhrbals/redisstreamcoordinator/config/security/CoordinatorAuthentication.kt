package io.github.ghkdqhrbals.redisstreamcoordinator.config.security

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.HttpHeaders
import java.nio.charset.StandardCharsets
import java.util.Base64

internal fun HttpServletRequest.authenticate(properties: CoordinatorProperties): AuthenticatedPrincipal? {
    val header = getHeader(HttpHeaders.AUTHORIZATION) ?: return null
    if (header.startsWith("Bearer ", ignoreCase = true)) {
        return CoordinatorApiTokenCodec(properties.api).decode(header.substring("Bearer ".length).trim())
    }
    if (!header.startsWith("Basic ", ignoreCase = true)) return null

    val decoded = runCatching {
        String(Base64.getDecoder().decode(header.substring("Basic ".length).trim()), StandardCharsets.UTF_8)
    }.getOrNull() ?: return null

    val separator = decoded.indexOf(':')
    if (separator < 0) return null

    val username = decoded.substring(0, separator)
    val password = decoded.substring(separator + 1)
    return properties.api.authenticate(username, password)
}

internal fun CoordinatorProperties.Api.authenticate(username: String, password: String): AuthenticatedPrincipal? =
    principals()
        .firstOrNull { it.username == username && it.password == password }
        ?.toPrincipal()

private data class ConfiguredPrincipal(
    val username: String,
    val password: String,
    val roles: Set<CoordinatorProperties.ApiRole>,
) {
    fun toPrincipal(): AuthenticatedPrincipal =
        AuthenticatedPrincipal(
            username = username,
            roles = roles,
        )
}

private fun CoordinatorProperties.Api.principals(): List<ConfiguredPrincipal> =
    if (users.isEmpty()) {
        listOf(
            ConfiguredPrincipal(
                username = adminUsername,
                password = adminPassword,
                roles = setOf(
                    CoordinatorProperties.ApiRole.READ,
                    CoordinatorProperties.ApiRole.WRITE,
                    CoordinatorProperties.ApiRole.ADMIN,
                    CoordinatorProperties.ApiRole.MONITOR,
                    CoordinatorProperties.ApiRole.MEMBER,
                ),
            ),
        )
    } else {
        users
            .filter { it.username.isNotBlank() }
            .map { user -> ConfiguredPrincipal(user.username, user.password, user.roles) }
    }
