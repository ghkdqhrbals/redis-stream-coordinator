package io.github.ghkdqhrbals.redisstreamcoordinator.config.web

import io.github.ghkdqhrbals.redisstreamcoordinator.config.AdminMutationRateLimitInterceptor
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditLogSink
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditLogger
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.config.audit.AuditLogInterceptor
import io.github.ghkdqhrbals.redisstreamcoordinator.config.security.CoordinatorAuthInterceptor
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorMetrics
import org.springframework.context.annotation.Configuration
import org.springframework.web.servlet.config.annotation.InterceptorRegistry
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer
import tools.jackson.databind.ObjectMapper

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
        registry.addInterceptor(CoordinatorAuthInterceptor(properties))
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
