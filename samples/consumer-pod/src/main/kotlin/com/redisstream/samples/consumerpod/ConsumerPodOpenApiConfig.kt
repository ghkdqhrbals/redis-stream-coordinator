package com.redisstream.samples.consumerpod

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Info
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration(proxyBeanMethods = false)
class ConsumerPodOpenApiConfig {
    @Bean
    fun consumerPodOpenApi(): OpenAPI =
        OpenAPI()
            .info(
                Info()
                    .title("Redis Stream Consumer Pod Sample API")
                    .version("v1")
                    .description("Sample API for inspecting consumer pod assignment events and publishing a test Redis Stream message through coordinator routing."),
            )
}
