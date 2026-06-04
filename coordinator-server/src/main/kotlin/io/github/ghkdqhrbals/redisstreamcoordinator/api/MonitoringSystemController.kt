package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.config.security.AuthRequestAttributes
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HealthResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MonitoringSessionResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.protocol.CoordinatorCompatibilityResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/monitoring")
@Tag(
    name = "Monitoring",
    description = "Read-only health, group, member, assignment, migration, shard, message, and Grafana dashboard APIs.",
)
class MonitoringSystemController(
    private val coordinator: CoordinatorService,
) {
    @Operation(
        operationId = "getMonitoringSession",
        summary = "Read monitoring session",
        description = "Returns the authenticated monitoring principal that the console and Grafana data source are using.",
        responses = [ApiResponse(responseCode = "200", description = "Monitoring session.")],
    )
    @GetMapping("/session")
    fun session(request: HttpServletRequest): MonitoringSessionResponse =
        MonitoringSessionResponse(
            authenticated = true,
            username = request.getAttribute(AuthRequestAttributes.PRINCIPAL) as? String,
            roles = (request.getAttribute(AuthRequestAttributes.ROLES) as? Iterable<*>)
                ?.mapNotNull { it as? String }
                ?: emptyList(),
        )

    @Operation(
        operationId = "getCoordinatorHealth",
        summary = "Read coordinator health",
        description = "Returns coordinator process health and Redis connectivity state.",
        responses = [ApiResponse(responseCode = "200", description = "Coordinator health.")],
    )
    @GetMapping("/health")
    fun health(): HealthResponse =
        coordinator.health()

    @Operation(
        operationId = "getCoordinatorCompatibility",
        summary = "Read coordinator compatibility",
        description = "Returns the coordination protocol version range and shared timing defaults supported by this coordinator build.",
        responses = [ApiResponse(responseCode = "200", description = "Compatibility metadata.")],
    )
    @GetMapping("/compatibility")
    fun compatibility(): CoordinatorCompatibilityResponse =
        coordinator.compatibility()
}
