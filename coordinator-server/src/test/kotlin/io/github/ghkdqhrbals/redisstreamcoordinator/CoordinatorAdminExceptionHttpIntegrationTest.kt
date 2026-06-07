package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateStreamRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.NoopStreamShardProvisioner
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.StreamShardProvisioner
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import tools.jackson.databind.ObjectMapper
import java.nio.charset.StandardCharsets
import java.util.Base64

@SpringBootTest(
    properties = [
        "coordinator.store.type=memory",
        "coordinator.streams.provisioning-enabled=true",
        "coordinator.defaults.initial-shard-count=2",
        "coordinator.api.admin-username=admin",
        "coordinator.api.admin-password=password",
    ],
)
class CoordinatorAdminExceptionHttpIntegrationTest {
    @Autowired
    private lateinit var webApplicationContext: WebApplicationContext

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setUpMockMvc() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build()
    }

    @Test
    fun `create stream returns conflict when Redis shard key already exists for prefix`() {
        mockMvc.perform(
            post("/coord/v1/streams/occupied-prefix")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(CreateStreamRequest(initialShardCount = 2, requestedBy = "test"))),
        )
            .andExpect(status().isConflict)
            .andExpect(jsonPath("$.errorCode").value(CoordinatorError.STREAM_PREFIX_ALREADY_EXISTS.code))
            .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("occupied-prefix:0")))
    }

    @Test
    fun `admin create stream validation errors use coordinator error response`() {
        mockMvc.perform(
            post("/coord/v1/streams/invalid-body")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(CreateStreamRequest(initialShardCount = 0, requestedBy = "test"))),
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.errorCode").value(CoordinatorError.INVALID_REQUEST.code))
            .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("initialShardCount")))
    }

    @Test
    fun `consumer concurrency is not exposed as an admin endpoint`() {
        mockMvc.perform(
            patch("/coord/v1/streams/create-payment/groups/payment-low-workers/consumer-concurrency")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"defaultMaxConcurrency":2,"requestedBy":"test","reason":"should not exist"}"""),
        )
            .andExpect(status().isNotFound)
    }

    private fun basicAuth(username: String = "admin", password: String = "password"): String {
        val token = "$username:$password".toByteArray(StandardCharsets.UTF_8)
        return "Basic ${Base64.getEncoder().encodeToString(token)}"
    }

    @TestConfiguration
    class TestConfig {
        @Bean
        @Primary
        fun fakeCoordinatorRedisCommands(): CoordinatorRedisCommands =
            object : CoordinatorRedisCommands() {
                override fun isConfigured(): Boolean =
                    true

                override fun hasKey(key: String): Boolean =
                    key == "occupied-prefix:0"
            }

        @Bean
        @Primary
        fun streamShardProvisioner(): StreamShardProvisioner =
            NoopStreamShardProvisioner
    }
}
