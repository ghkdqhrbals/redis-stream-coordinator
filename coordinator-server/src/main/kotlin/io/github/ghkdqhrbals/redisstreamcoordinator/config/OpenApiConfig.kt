package io.github.ghkdqhrbals.redisstreamcoordinator.config

import com.scalar.maven.webmvc.ScalarWebMvcController
import com.scalar.maven.webmvc.SpringBootScalarProperties
import io.swagger.v3.oas.models.Components
import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import io.swagger.v3.oas.models.security.SecurityRequirement
import io.swagger.v3.oas.models.security.SecurityScheme
import io.swagger.v3.oas.models.tags.Tag
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.boot.context.properties.EnableConfigurationProperties

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SpringBootScalarProperties::class)
class OpenApiConfig {
    @Bean
    fun scalarWebMvcController(): ScalarWebMvcController =
        ScalarWebMvcController()

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
            .tags(
                listOf(
                    Tag()
                        .name("Group Administration")
                        .description("Create, read, and delete coordinator-owned stream group metadata."),
                    Tag()
                        .name("Group Operations")
                        .description("Producer routing, resharding, consumer concurrency, and migration rollback operations."),
                    Tag()
                        .name("Consumer Heartbeats")
                        .description("Consumer join, lease renewal, assignment reconciliation, revoke progress, and progress reporting."),
                    Tag()
                        .name("Monitoring")
                        .description("Read-only health, group, member, assignment, migration, shard, message, and Grafana dashboard APIs."),
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
