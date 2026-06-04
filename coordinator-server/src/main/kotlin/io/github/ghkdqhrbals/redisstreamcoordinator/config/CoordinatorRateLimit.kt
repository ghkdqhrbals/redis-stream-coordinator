package io.github.ghkdqhrbals.redisstreamcoordinator.config

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.web.servlet.HandlerInterceptor
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.ceil
import kotlin.math.max

class AdminMutationRateLimitInterceptor(
    private val properties: CoordinatorProperties.RateLimit,
    private val limiter: FixedWindowRateLimiter = FixedWindowRateLimiter(),
) : HandlerInterceptor {
    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        val target = request.adminMutationTarget() ?: return true
        val principal = request.getAttribute(AUTH_PRINCIPAL_ATTRIBUTE) as? String ?: "unknown"
        val decision = limiter.tryAcquire(
            key = "$principal:${target.streamPrefix}:${target.consumerGroup}",
            maxPermits = properties.adminMutationsPerMinute,
            window = Duration.ofMinutes(1),
        )

        if (decision.allowed) {
            return true
        }

        val error = CoordinatorError.RATE_LIMIT_EXCEEDED
        response.status = error.status.value()
        response.setHeader(HttpHeaders.RETRY_AFTER, decision.retryAfterSeconds.toString())
        response.contentType = MediaType.APPLICATION_JSON_VALUE
        response.characterEncoding = Charsets.UTF_8.name()
        response.writer.write(
            """
            {"status":"${error.status.name}","errorCode":"${error.code}","message":"${error.defaultMessage}"}
            """.trimIndent(),
        )
        return false
    }
}

data class RateLimitDecision(
    val allowed: Boolean,
    val retryAfterSeconds: Long,
)

class FixedWindowRateLimiter(
    private val clock: Clock = Clock.systemUTC(),
) {
    private val windows = ConcurrentHashMap<String, Window>()

    fun tryAcquire(key: String, maxPermits: Int, window: Duration): RateLimitDecision {
        val nowMillis = clock.millis()
        val windowMillis = window.toMillis()
        if (maxPermits <= 0) {
            return RateLimitDecision(allowed = false, retryAfterSeconds = window.seconds)
        }

        var allowed = false
        var resetAtMillis = nowMillis + windowMillis
        windows.compute(key) { _, current ->
            val activeWindow = when {
                current == null -> Window(startMillis = nowMillis, usedPermits = 0)
                nowMillis - current.startMillis >= windowMillis -> Window(startMillis = nowMillis, usedPermits = 0)
                else -> current
            }
            resetAtMillis = activeWindow.startMillis + windowMillis
            if (activeWindow.usedPermits < maxPermits) {
                allowed = true
                activeWindow.copy(usedPermits = activeWindow.usedPermits + 1)
            } else {
                activeWindow
            }
        }

        return RateLimitDecision(
            allowed = allowed,
            retryAfterSeconds = max(1, ceil((resetAtMillis - nowMillis).toDouble() / 1_000).toLong()),
        )
    }

    private data class Window(
        val startMillis: Long,
        val usedPermits: Int,
    )
}

private data class AdminMutationTarget(
    val streamPrefix: String,
    val consumerGroup: String,
)

private fun HttpServletRequest.adminMutationTarget(): AdminMutationTarget? {
    if (method.uppercase() !in ADMIN_MUTATION_METHODS) {
        return null
    }

    val parts = requestURI.removePrefix("/").split("/")
    if (parts.size < 4 || parts[0] != "coord" || parts[1] != "v1" || parts[2] != "streams") {
        return null
    }

    if (parts.size == 4 && method.equals("POST", ignoreCase = true)) {
        return AdminMutationTarget(streamPrefix = parts[3], consumerGroup = "*")
    }

    if (parts.size == 5 && parts[4] == "scale") {
        return AdminMutationTarget(streamPrefix = parts[3], consumerGroup = "*")
    }

    if (parts.size < 6 || parts[4] != "groups") {
        return null
    }

    val tail = parts.drop(6)
    if (tail.size == 3 && tail[0] == "members" && tail[2] == "heartbeat") {
        return null
    }

    return AdminMutationTarget(streamPrefix = parts[3], consumerGroup = parts[5])
}

private val ADMIN_MUTATION_METHODS = setOf("POST", "PATCH", "PUT", "DELETE")
