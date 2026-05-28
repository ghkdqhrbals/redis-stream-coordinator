package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RuntimeConsumerCapacity
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.header
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
        "coordinator.api.admin-username=admin",
        "coordinator.api.admin-password=password",
        "coordinator.api.authenticate-member-api=false",
        "coordinator.api.rate-limit.enabled=true",
        "coordinator.api.rate-limit.admin-mutations-per-minute=1",
    ],
)
class CoordinatorRateLimitHttpIntegrationTest {
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
    fun `rate limit rejects repeated admin mutations for same caller and group`() {
        mockMvc.perform(
            post("/coord/v1/streams/rate-limit-admin/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 1))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            post("/coord/v1/streams/rate-limit-admin/groups/orders-consumer/scale")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"targetShardCount":2,"requestedBy":"test","reason":"rate limit"}"""),
        )
            .andExpect(status().isTooManyRequests)
            .andExpect(header().exists(HttpHeaders.RETRY_AFTER))
            .andExpect(jsonPath("$.errorCode").value(CoordinatorError.RATE_LIMIT_EXCEEDED.code))
    }

    @Test
    fun `rate limit does not block monitoring reads or member heartbeats`() {
        mockMvc.perform(
            post("/coord/v1/streams/rate-limit-read-heartbeat/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 1))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            get("/coord/v1/monitoring/streams/rate-limit-read-heartbeat/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)

        mockMvc.perform(
            post("/coord/v1/streams/rate-limit-read-heartbeat/groups/orders-consumer/members/member-a/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(heartbeat("member-a"))),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value("OK"))
    }

    private fun basicAuth(username: String = "admin", password: String = "password"): String {
        val token = "$username:$password".toByteArray(StandardCharsets.UTF_8)
        return "Basic ${Base64.getEncoder().encodeToString(token)}"
    }

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun heartbeat(memberId: String): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = 0,
            rebalanceTimeoutMs = 60_000,
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
        )
}
