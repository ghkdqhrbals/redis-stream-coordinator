package io.github.ghkdqhrbals.redisstreamcoordinator.config.security

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.config.web.isMutationMethod
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.http.HttpHeaders
import org.springframework.web.servlet.HandlerInterceptor

class CoordinatorAuthInterceptor(
    private val properties: CoordinatorProperties,
) : HandlerInterceptor {
    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        val requiredRole = requiredRole(request.requestURI, request.method)
        if (requiredRole == null) {
            return true
        }

        val principal = request.authenticate(properties)
        if (principal == null) {
            request.setAttribute(AuthRequestAttributes.FAILURE, "UNAUTHORIZED")
            response.status = HttpServletResponse.SC_UNAUTHORIZED
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, """Basic realm="redis-stream-coordinator"""")
            return false
        }

        request.setAttribute(AuthRequestAttributes.PRINCIPAL, principal.username)
        request.setAttribute(AuthRequestAttributes.ROLES, principal.roles.map { it.name }.sorted())
        if (principal.hasRole(requiredRole)) {
            return true
        }

        request.setAttribute(AuthRequestAttributes.FAILURE, "FORBIDDEN")
        response.status = HttpServletResponse.SC_FORBIDDEN
        return false
    }

    private fun requiredRole(path: String, method: String): CoordinatorProperties.ApiRole? {
        if (path == "/coord/v1/auth/login") {
            return null
        }
        if (path.contains("/members/") && path.endsWith("/heartbeat")) {
            return if (properties.api.authenticateMemberApi) CoordinatorProperties.ApiRole.MEMBER else null
        }
        if (path.startsWith("/coord/v1/monitoring")) {
            return CoordinatorProperties.ApiRole.READ
        }
        if (path.startsWith("/coord/v1/streams")) {
            return if (method.isMutationMethod()) {
                CoordinatorProperties.ApiRole.WRITE
            } else {
                CoordinatorProperties.ApiRole.READ
            }
        }
        return when (method.uppercase()) {
            "POST", "PATCH", "PUT", "DELETE" -> CoordinatorProperties.ApiRole.WRITE
            else -> CoordinatorProperties.ApiRole.READ
        }
    }
}
