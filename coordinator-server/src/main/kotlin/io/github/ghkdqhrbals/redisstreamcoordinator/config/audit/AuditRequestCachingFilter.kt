package io.github.ghkdqhrbals.redisstreamcoordinator.config.audit

import io.github.ghkdqhrbals.redisstreamcoordinator.config.web.isMutationMethod
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import org.springframework.web.util.ContentCachingRequestWrapper

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

    private companion object {
        const val AUDIT_REQUEST_BODY_CACHE_LIMIT_BYTES = 64 * 1024
    }
}
