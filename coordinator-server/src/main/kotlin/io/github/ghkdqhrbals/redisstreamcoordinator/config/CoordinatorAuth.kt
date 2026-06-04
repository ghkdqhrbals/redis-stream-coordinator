package io.github.ghkdqhrbals.redisstreamcoordinator.config

import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorMetrics
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.context.annotation.Configuration
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.util.ContentCachingRequestWrapper
import org.springframework.web.servlet.HandlerInterceptor
import org.springframework.web.servlet.HandlerMapping
import org.springframework.web.servlet.config.annotation.InterceptorRegistry
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.time.Instant
import java.util.Base64

@Configuration
class CoordinatorWebConfig(
    private val properties: CoordinatorProperties,
    private val auditLogSink: CoordinatorAuditLogSink,
    private val metrics: CoordinatorMetrics,
    private val objectMapper: ObjectMapper,
) : WebMvcConfigurer {
    override fun addInterceptors(registry: InterceptorRegistry) {
        registry.addInterceptor(ApiMetricsInterceptor(metrics))
            .addPathPatterns("/coord/v1/**")
        registry.addInterceptor(
            AuditLogInterceptor(
                auditLogger = CoordinatorAuditLogger(properties, auditLogSink),
                objectMapper = objectMapper,
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

    override fun addViewControllers(registry: ViewControllerRegistry) {
        registry.addViewController("/console").setViewName("forward:/console/index.html")
        registry.addViewController("/console/").setViewName("forward:/console/index.html")
    }
}

@Component
class AuditRequestCachingFilter : OncePerRequestFilter() {
    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        if (request.requestURI.startsWith("/coord/v1/streams") && request.method.isMutationMethod()) {
            filterChain.doFilter(ContentCachingRequestWrapper(request, AUDIT_REQUEST_BODY_CACHE_LIMIT_BYTES), response)
        } else {
            filterChain.doFilter(request, response)
        }
    }
}

class ApiMetricsInterceptor(
    private val metrics: CoordinatorMetrics,
) : HandlerInterceptor {
    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        request.setAttribute(API_METRICS_STARTED_AT_ATTRIBUTE, Instant.now())
        return true
    }

    override fun afterCompletion(
        request: HttpServletRequest,
        response: HttpServletResponse,
        handler: Any,
        ex: Exception?,
    ) {
        val startedAt = request.getAttribute(API_METRICS_STARTED_AT_ATTRIBUTE) as? Instant ?: return
        val route = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE) as? String
            ?: request.requestURI
        val group = request.apiMetricGroup()
        metrics.recordApiRequest(
            method = request.method,
            route = route,
            status = response.status,
            outcome = when {
                ex != null -> "ERROR"
                response.status < 400 -> "SUCCESS"
                response.status < 500 -> "CLIENT_ERROR"
                else -> "SERVER_ERROR"
            },
            streamPrefix = group?.first,
            consumerGroup = group?.second,
            duration = Duration.between(startedAt, Instant.now()),
        )
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
        request.setAttribute(AUTH_ROLES_ATTRIBUTE, principal.roles.map { it.name }.sorted())
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
            return CoordinatorProperties.ApiRole.READ
        }
        if (path.startsWith("/coord/v1/streams")) {
            return CoordinatorProperties.ApiRole.WRITE
        }
        return when (method.uppercase()) {
            "POST", "PATCH", "PUT", "DELETE" -> CoordinatorProperties.ApiRole.WRITE
            else -> CoordinatorProperties.ApiRole.READ
        }
    }
}

class AuditLogInterceptor(
    private val auditLogger: CoordinatorAuditLogger,
    private val objectMapper: ObjectMapper,
) : HandlerInterceptor {
    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        request.setAttribute(AUDIT_STARTED_AT_ATTRIBUTE, Instant.now())
        return true
    }

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
        val body = request.cachedBody()
        val auditBody = body?.let(::parseAuditBody)
        val startedAt = request.getAttribute(AUDIT_STARTED_AT_ATTRIBUTE) as? Instant
        val route = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE) as? String

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
            route = route,
            queryString = request.queryString,
            requestId = request.requestId(),
            requestedBy = auditBody?.requestedBy,
            reason = auditBody?.reason,
            clientAddress = request.clientAddress(),
            userAgent = request.getHeader(HttpHeaders.USER_AGENT),
            roles = request.authRoles(),
            durationMs = startedAt?.let { Duration.between(it, Instant.now()).toMillis().coerceAtLeast(0) },
            requestBodySha256 = body?.takeIf { it.isNotEmpty() }?.sha256Hex(),
            requestSummary = auditBody?.summary ?: emptyMap(),
        )
    }

    private fun parseAuditBody(body: ByteArray): AuditRequestBody? =
        runCatching {
            val root = objectMapper.readTree(String(body, StandardCharsets.UTF_8))
                .takeIf(JsonNode::isObject)
                ?: return null
            AuditRequestBody(
                requestedBy = root.textField("requestedBy"),
                reason = root.textField("reason"),
                summary = listOf(
                    "initialShardCount",
                    "targetShardCount",
                    "defaultMaxConcurrency",
                    "force",
                ).mapNotNull { key -> root.get(key)?.asString()?.let { value -> key to value } }.toMap(),
            )
        }.getOrNull()
}

internal const val AUTH_PRINCIPAL_ATTRIBUTE = "redisStreamCoordinator.auth.principal"
internal const val AUTH_ROLES_ATTRIBUTE = "redisStreamCoordinator.auth.roles"
private const val AUTH_FAILURE_ATTRIBUTE = "redisStreamCoordinator.auth.failure"
private const val API_METRICS_STARTED_AT_ATTRIBUTE = "redisStreamCoordinator.apiMetrics.startedAt"
private const val AUDIT_STARTED_AT_ATTRIBUTE = "redisStreamCoordinator.audit.startedAt"
private const val AUDIT_REQUEST_BODY_CACHE_LIMIT_BYTES = 64 * 1024

private data class AuditRequestBody(
    val requestedBy: String?,
    val reason: String?,
    val summary: Map<String, String>,
)

private data class AuthenticatedPrincipal(
    val username: String,
    val password: String,
    val roles: Set<CoordinatorProperties.ApiRole>,
) {
    fun hasRole(requiredRole: CoordinatorProperties.ApiRole): Boolean =
        roles.any { role -> role.grants(requiredRole) }
}

private data class AuditTarget(
    val action: CoordinatorAuditAction,
    val streamPrefix: String,
    val consumerGroup: String?,
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
            .map { user -> AuthenticatedPrincipal(user.username, user.password, user.roles) }
    }

private fun CoordinatorProperties.ApiRole.grants(requiredRole: CoordinatorProperties.ApiRole): Boolean =
    when (this) {
        CoordinatorProperties.ApiRole.WRITE ->
            requiredRole == CoordinatorProperties.ApiRole.WRITE ||
                requiredRole == CoordinatorProperties.ApiRole.READ
        CoordinatorProperties.ApiRole.READ ->
            requiredRole == CoordinatorProperties.ApiRole.READ
        CoordinatorProperties.ApiRole.MEMBER ->
            requiredRole == CoordinatorProperties.ApiRole.MEMBER
        CoordinatorProperties.ApiRole.ADMIN ->
            true
        CoordinatorProperties.ApiRole.MONITOR ->
            requiredRole == CoordinatorProperties.ApiRole.READ
    }

private fun HttpServletRequest.auditTarget(): AuditTarget? {
    val path = requestURI.removePrefix("/")
    val parts = path.split("/")
    if (parts.size < 4 || parts[0] != "coord" || parts[1] != "v1" || parts[2] != "streams") {
        return null
    }

    if (method.equals("POST", ignoreCase = true) && parts.size == 4) {
        return AuditTarget(CoordinatorAuditAction.CREATE_STREAM, parts[3], consumerGroup = null)
    }

    if (method.equals("POST", ignoreCase = true) && parts.size == 5 && parts[4] == "scale") {
        return AuditTarget(CoordinatorAuditAction.SCALE_STREAM, parts[3], consumerGroup = null)
    }

    if (parts.size < 6 || parts[4] != "groups") {
        return null
    }

    val streamPrefix = parts[3]
    val consumerGroup = parts[5]
    val tail = parts.drop(6)
    return when {
        method.equals("POST", ignoreCase = true) && tail.isEmpty() ->
            AuditTarget(CoordinatorAuditAction.CREATE_GROUP, streamPrefix, consumerGroup)
        method.equals("DELETE", ignoreCase = true) && tail.isEmpty() ->
            AuditTarget(CoordinatorAuditAction.DELETE_GROUP, streamPrefix, consumerGroup)
        method.equals("POST", ignoreCase = true) && tail == listOf("scale") ->
            AuditTarget(CoordinatorAuditAction.SCALE_GROUP, streamPrefix, consumerGroup)
        method.equals("PATCH", ignoreCase = true) && tail == listOf("consumer-concurrency") ->
            AuditTarget(CoordinatorAuditAction.UPDATE_CONSUMER_CONCURRENCY, streamPrefix, consumerGroup)
        method.equals("POST", ignoreCase = true) && tail.size == 3 && tail[0] == "migrations" && tail[2] == "rollback" ->
            AuditTarget(CoordinatorAuditAction.ROLLBACK_MIGRATION, streamPrefix, consumerGroup, reshardingId = tail[1])
        else -> null
    }
}

private fun String.isMutationMethod(): Boolean =
    equals("POST", ignoreCase = true) ||
        equals("PATCH", ignoreCase = true) ||
        equals("PUT", ignoreCase = true) ||
        equals("DELETE", ignoreCase = true)

private fun HttpServletRequest.cachedBody(): ByteArray? =
    (this as? ContentCachingRequestWrapper)
        ?.contentAsByteArray
        ?.takeIf { it.isNotEmpty() }

private fun HttpServletRequest.requestId(): String? =
    getHeader("X-Request-Id")
        ?: getHeader("X-Correlation-Id")
        ?: getHeader("X-Amzn-Trace-Id")

private fun HttpServletRequest.authRoles(): List<String> =
    (getAttribute(AUTH_ROLES_ATTRIBUTE) as? Iterable<*>)
        ?.filterIsInstance<String>()
        .orEmpty()

private fun HttpServletRequest.clientAddress(): String? =
    getHeader("X-Forwarded-For")
        ?.split(",")
        ?.firstOrNull()
        ?.trim()
        ?.takeIf { it.isNotBlank() }
        ?: remoteAddr

private fun JsonNode.textField(name: String): String? =
    get(name)?.asString()?.takeIf { it.isNotBlank() }

private fun ByteArray.sha256Hex(): String =
    MessageDigest.getInstance("SHA-256")
        .digest(this)
        .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }

private fun HttpServletRequest.apiMetricGroup(): Pair<String, String>? {
    val parts = requestURI.removePrefix("/").split("/")
    if (parts.size >= 6 && parts[0] == "coord" && parts[1] == "v1" && parts[2] == "streams" && parts[4] == "groups") {
        return parts[3] to parts[5]
    }
    if (
        parts.size >= 8 &&
        parts[0] == "coord" &&
        parts[1] == "v1" &&
        parts[2] == "monitoring" &&
        parts[3] == "streams" &&
        parts[5] == "groups"
    ) {
        return parts[4] to parts[6]
    }
    return null
}
