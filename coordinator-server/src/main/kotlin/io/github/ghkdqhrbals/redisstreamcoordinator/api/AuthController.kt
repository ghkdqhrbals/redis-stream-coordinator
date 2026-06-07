package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.config.security.CoordinatorApiTokenCodec
import io.github.ghkdqhrbals.redisstreamcoordinator.config.security.authenticate
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.LoginRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.LoginResponse
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.servlet.http.HttpServletRequest
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

@RestController
@RequestMapping("/coord/v1/auth")
@Tag(
    name = "Authentication",
    description = "Issue short-lived coordinator API tokens for console, Grafana, and administrative API calls.",
)
class AuthController(
    private val properties: CoordinatorProperties,
) {
    @Operation(
        operationId = "loginCoordinatorApi",
        summary = "Issue coordinator API token",
        description = "Authenticates a configured coordinator user and returns a signed Bearer token. The default token lifetime is seven days.",
        responses = [
            ApiResponse(responseCode = "200", description = "Bearer token issued."),
            ApiResponse(responseCode = "401", description = "Invalid credentials."),
        ],
    )
    @PostMapping("/login")
    fun login(
        request: HttpServletRequest,
        @RequestBody(required = false) body: LoginRequest?,
    ): LoginResponse {
        val principal = when {
            body != null && body.username.isNotBlank() ->
                properties.api.authenticate(body.username, body.password)
            else ->
                request.authenticate(properties)
        } ?: throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid coordinator credentials")

        val issued = CoordinatorApiTokenCodec(properties.api).issue(principal)
        return LoginResponse(
            accessToken = issued.token,
            tokenType = issued.tokenType,
            expiresAt = issued.expiresAt,
            expiresInSeconds = issued.expiresInSeconds,
            roles = issued.roles,
        )
    }
}
