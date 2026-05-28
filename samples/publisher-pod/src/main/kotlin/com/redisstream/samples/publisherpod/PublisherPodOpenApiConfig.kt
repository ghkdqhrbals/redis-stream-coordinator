package com.redisstream.samples.publisherpod

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class PublisherPodOpenApiConfig {
    @Bean
    fun publisherPodOpenApi(): OpenAPI =
        OpenAPI()
            .info(
                Info()
                    .title("Redis Stream Publisher Pod Sample API")
                    .version("v1")
                    .description("Sample API for inspecting publisher status and sending test messages through coordinator-managed Redis Stream routing."),
            )
}
