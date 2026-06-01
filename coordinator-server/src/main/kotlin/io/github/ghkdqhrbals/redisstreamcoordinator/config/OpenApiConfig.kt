package io.github.ghkdqhrbals.redisstreamcoordinator.config

import io.swagger.v3.oas.models.Components
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.security.SecurityRequirement
import io.swagger.v3.oas.models.security.SecurityScheme
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class OpenApiConfig {
    @Bean
    fun coordinatorOpenApi(): OpenAPI =
        OpenAPI()
            .info(
                Info()
                    .title("Redis Stream Coordinator API")
                    .version("v1")
                    .description(
                        """
                        Control-plane API for Redis Stream shard groups, member heartbeats, producer routing, resharding, monitoring, ACL, and audit operations.

                        Redis Stream Coordinator exists because a single Redis Stream key can become a BigKey and a Redis Cluster hash-slot hotspot. The coordinator treats one logical stream as multiple physical shard streams and owns the membership, assignment, revoke-before-assign, producer routing, and monitoring protocol around those shards.
                        """.trimIndent(),
                    ),
            )
            .components(
                Components()
                    .addSecuritySchemes(
                        BASIC_AUTH_SCHEME,
                        SecurityScheme()
                            .type(SecurityScheme.Type.HTTP)
                            .scheme("basic"),
                    ),
            )
            .addSecurityItem(SecurityRequirement().addList(BASIC_AUTH_SCHEME))

    private companion object {
        const val BASIC_AUTH_SCHEME = "basicAuth"
    }
}
