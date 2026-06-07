package io.github.ghkdqhrbals.redisstreamcoordinator.config.audit

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditLogger
import io.github.ghkdqhrbals.redisstreamcoordinator.config.security.AuthRequestAttributes
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.http.HttpHeaders
import org.springframework.web.servlet.HandlerInterceptor
import org.springframework.web.servlet.HandlerMapping
import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant

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
            request.getAttribute(AuthRequestAttributes.FAILURE) == "FORBIDDEN" -> "FORBIDDEN"
            request.getAttribute(AuthRequestAttributes.FAILURE) == "UNAUTHORIZED" -> "UNAUTHORIZED"
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
            principal = request.getAttribute(AuthRequestAttributes.PRINCIPAL) as? String,
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
                    "force",
                ).mapNotNull { key -> root.get(key)?.asString()?.let { value -> key to value } }.toMap(),
            )
        }.getOrNull()

    private companion object {
        const val AUDIT_STARTED_AT_ATTRIBUTE = "redisStreamCoordinator.audit.startedAt"
    }
}

private data class AuditRequestBody(
    val requestedBy: String?,
    val reason: String?,
    val summary: Map<String, String>,
)

private fun HttpServletRequest.cachedBody(): ByteArray? =
    (this as? org.springframework.web.util.ContentCachingRequestWrapper)
        ?.contentAsByteArray
        ?.takeIf { it.isNotEmpty() }

private fun HttpServletRequest.requestId(): String? =
    getHeader("X-Request-Id")
        ?: getHeader("X-Correlation-Id")
        ?: getHeader("X-Amzn-Trace-Id")

private fun HttpServletRequest.authRoles(): List<String> =
    (getAttribute(AuthRequestAttributes.ROLES) as? Iterable<*>)
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
