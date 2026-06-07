package io.github.ghkdqhrbals.redisstreamcoordinator.config.web

import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorMetrics
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.web.servlet.HandlerInterceptor
import org.springframework.web.servlet.HandlerMapping
import java.time.Duration
import java.time.Instant

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

    private companion object {
        const val API_METRICS_STARTED_AT_ATTRIBUTE = "redisStreamCoordinator.apiMetrics.startedAt"
    }
}
