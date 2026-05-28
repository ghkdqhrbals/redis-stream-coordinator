package io.github.ghkdqhrbals.redisstreamcoordinator.config

import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.web.servlet.HandlerInterceptor
import org.springframework.web.servlet.config.annotation.InterceptorRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import java.nio.charset.StandardCharsets
import java.util.Base64

@Configuration
class CoordinatorWebConfig(
    private val properties: CoordinatorProperties,
    private val auditLogSink: CoordinatorAuditLogSink,
) : WebMvcConfigurer {
    override fun addInterceptors(registry: InterceptorRegistry) {
        registry.addInterceptor(
            AuditLogInterceptor(
                auditLogger = CoordinatorAuditLogger(properties, auditLogSink),
            ),
        )
            .addPathPatterns("/coord/v1/**")
        registry.addInterceptor(BasicAuthInterceptor(properties))
            .addPathPatterns("/coord/v1/**")
        if (properties.api.rateLimit.enabled) {
            registry.addInterceptor(AdminMutationRateLimitInterceptor(properties.api.rateLimit))
                .addPathPatterns("/coord/v1/**")
        }
    }
}

class BasicAuthInterceptor(
    private val properties: CoordinatorProperties,
) : HandlerInterceptor {
    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        val requiredRole = requiredRole(request.requestURI, request.method)
        if (requiredRole == null) {
            return true
        }

        val principal = request.authenticate(properties)
        if (principal == null) {
            request.setAttribute(AUTH_FAILURE_ATTRIBUTE, "UNAUTHORIZED")
            response.status = HttpServletResponse.SC_UNAUTHORIZED
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, """Basic realm="redis-stream-coordinator"""")
            return false
        }

        request.setAttribute(AUTH_PRINCIPAL_ATTRIBUTE, principal.username)
        if (principal.hasRole(requiredRole)) {
            return true
        }

        request.setAttribute(AUTH_FAILURE_ATTRIBUTE, "FORBIDDEN")
        response.status = HttpServletResponse.SC_FORBIDDEN
        return false
    }

    private fun requiredRole(path: String, method: String): CoordinatorProperties.ApiRole? {
        if (path.contains("/members/") && path.endsWith("/heartbeat")) {
            return if (properties.api.authenticateMemberApi) CoordinatorProperties.ApiRole.MEMBER else null
        }
        if (path.startsWith("/coord/v1/monitoring")) {
            return CoordinatorProperties.ApiRole.MONITOR
        }
        return when (method.uppercase()) {
            "POST", "PATCH", "PUT", "DELETE" -> CoordinatorProperties.ApiRole.ADMIN
            else -> CoordinatorProperties.ApiRole.MONITOR
        }
    }
}

class AuditLogInterceptor(
    private val auditLogger: CoordinatorAuditLogger,
) : HandlerInterceptor {
    override fun afterCompletion(
        request: HttpServletRequest,
        response: HttpServletResponse,
        handler: Any,
        ex: Exception?,
    ) {
        val target = request.auditTarget() ?: return
        val outcome = when {
            ex != null -> "ERROR"
            request.getAttribute(AUTH_FAILURE_ATTRIBUTE) == "FORBIDDEN" -> "FORBIDDEN"
            request.getAttribute(AUTH_FAILURE_ATTRIBUTE) == "UNAUTHORIZED" -> "UNAUTHORIZED"
            response.status in 200..399 -> "SUCCESS"
            else -> "FAILED"
        }

        auditLogger.append(
            action = target.action,
            outcome = outcome,
            status = response.status,
            principal = request.getAttribute(AUTH_PRINCIPAL_ATTRIBUTE) as? String,
            method = request.method,
            path = request.requestURI,
            streamPrefix = target.streamPrefix,
            consumerGroup = target.consumerGroup,
            reshardingId = target.reshardingId,
        )
    }
}

internal const val AUTH_PRINCIPAL_ATTRIBUTE = "redisStreamCoordinator.auth.principal"
private const val AUTH_FAILURE_ATTRIBUTE = "redisStreamCoordinator.auth.failure"

private data class AuthenticatedPrincipal(
    val username: String,
    val password: String,
    val roles: Set<CoordinatorProperties.ApiRole>,
) {
    fun hasRole(requiredRole: CoordinatorProperties.ApiRole): Boolean =
        CoordinatorProperties.ApiRole.ADMIN in roles || requiredRole in roles
}

private data class AuditTarget(
    val action: CoordinatorAuditAction,
    val streamPrefix: String,
    val consumerGroup: String,
    val reshardingId: String? = null,
)

private fun HttpServletRequest.authenticate(properties: CoordinatorProperties): AuthenticatedPrincipal? {
    val header = getHeader(HttpHeaders.AUTHORIZATION) ?: return null
    if (!header.startsWith("Basic ", ignoreCase = true)) return null

    val decoded = runCatching {
        String(Base64.getDecoder().decode(header.substring("Basic ".length).trim()), StandardCharsets.UTF_8)
    }.getOrNull() ?: return null

    val separator = decoded.indexOf(':')
    if (separator < 0) return null

    val username = decoded.substring(0, separator)
    val password = decoded.substring(separator + 1)
    return properties.api.principals()
        .firstOrNull { it.username == username && it.password == password }
}

private fun CoordinatorProperties.Api.principals(): List<AuthenticatedPrincipal> =
    if (users.isEmpty()) {
        listOf(
            AuthenticatedPrincipal(
                username = adminUsername,
                password = adminPassword,
                roles = setOf(
                    CoordinatorProperties.ApiRole.ADMIN,
                    CoordinatorProperties.ApiRole.MONITOR,
                    CoordinatorProperties.ApiRole.MEMBER,
                ),
            ),
        )
    } else {
        users
            .filter { it.username.isNotBlank() }
            .map { user -> AuthenticatedPrincipal(user.username, user.password, user.roles) }
    }

private fun HttpServletRequest.auditTarget(): AuditTarget? {
    val path = requestURI.removePrefix("/")
    val parts = path.split("/")
    if (parts.size < 6 || parts[0] != "coord" || parts[1] != "v1" || parts[2] != "streams" || parts[4] != "groups") {
        return null
    }

    val streamPrefix = parts[3]
    val consumerGroup = parts[5]
    val tail = parts.drop(6)
    return when {
        method.equals("POST", ignoreCase = true) && tail.isEmpty() ->
            AuditTarget(CoordinatorAuditAction.CREATE_GROUP, streamPrefix, consumerGroup)
        method.equals("POST", ignoreCase = true) && tail == listOf("scale") ->
            AuditTarget(CoordinatorAuditAction.SCALE_GROUP, streamPrefix, consumerGroup)
        method.equals("PATCH", ignoreCase = true) && tail == listOf("consumer-concurrency") ->
            AuditTarget(CoordinatorAuditAction.UPDATE_CONSUMER_CONCURRENCY, streamPrefix, consumerGroup)
        method.equals("POST", ignoreCase = true) && tail.size == 3 && tail[0] == "migrations" && tail[2] == "rollback" ->
            AuditTarget(CoordinatorAuditAction.ROLLBACK_MIGRATION, streamPrefix, consumerGroup, reshardingId = tail[1])
        else -> null
    }
}
