package io.github.ghkdqhrbals.redisstreamcoordinator

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
) : WebMvcConfigurer {
    override fun addInterceptors(registry: InterceptorRegistry) {
        registry.addInterceptor(BasicAuthInterceptor(properties))
            .addPathPatterns("/coord/v1/**")
    }
}

class BasicAuthInterceptor(
    private val properties: CoordinatorProperties,
) : HandlerInterceptor {
    override fun preHandle(request: HttpServletRequest, response: HttpServletResponse, handler: Any): Boolean {
        if (!requiresAuthentication(request.requestURI)) {
            return true
        }
        if (request.authorizationMatches(properties.api.adminUsername, properties.api.adminPassword)) {
            return true
        }
        response.status = HttpServletResponse.SC_UNAUTHORIZED
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, """Basic realm="redis-stream-coordinator"""")
        return false
    }

    private fun requiresAuthentication(path: String): Boolean {
        if (path.contains("/members/") && path.endsWith("/heartbeat")) {
            return properties.api.authenticateMemberApi
        }
        return true
    }
}

private fun HttpServletRequest.authorizationMatches(username: String, password: String): Boolean {
    val header = getHeader(HttpHeaders.AUTHORIZATION) ?: return false
    if (!header.startsWith("Basic ", ignoreCase = true)) return false

    val decoded = runCatching {
        String(Base64.getDecoder().decode(header.removePrefix("Basic ").trim()), StandardCharsets.UTF_8)
    }.getOrNull() ?: return false

    val separator = decoded.indexOf(':')
    if (separator < 0) return false
    return decoded.substring(0, separator) == username && decoded.substring(separator + 1) == password
}
