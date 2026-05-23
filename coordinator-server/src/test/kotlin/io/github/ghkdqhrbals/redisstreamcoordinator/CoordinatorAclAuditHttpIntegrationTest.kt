package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditAction
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditEvent
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditLogSink
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RuntimeConsumerCapacity
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.context.WebApplicationContext
import tools.jackson.databind.ObjectMapper
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals

@SpringBootTest(
    properties = [
        "coordinator.store.type=memory",
        "coordinator.api.authenticate-member-api=true",
        "coordinator.api.users[0].username=admin",
        "coordinator.api.users[0].password=admin-password",
        "coordinator.api.users[0].roles[0]=ADMIN",
        "coordinator.api.users[1].username=monitor",
        "coordinator.api.users[1].password=monitor-password",
        "coordinator.api.users[1].roles[0]=MONITOR",
        "coordinator.api.users[2].username=member",
        "coordinator.api.users[2].password=member-password",
        "coordinator.api.users[2].roles[0]=MEMBER",
    ],
)
class CoordinatorAclAuditHttpIntegrationTest {
    @Autowired
    private lateinit var webApplicationContext: WebApplicationContext

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var auditLogSink: RecordingAuditLogSink

    private lateinit var mockMvc: MockMvc

    @BeforeEach
    fun setUpMockMvc() {
        auditLogSink.clear()
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build()
    }

    @Test
    fun `acl allows monitor reads but rejects admin mutations`() {
        mockMvc.perform(
            get("/coord/v1/monitoring/groups")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("monitor", "monitor-password")),
        )
            .andExpect(status().isOk)

        mockMvc.perform(
            post("/coord/v1/streams/acl-denied/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("monitor", "monitor-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest())),
        )
            .andExpect(status().isForbidden)

        assertEquals(
            listOf(
                CoordinatorAuditAction.CREATE_GROUP to "FORBIDDEN",
            ),
            auditLogSink.events.map { it.action to it.outcome },
        )
        assertEquals("monitor", auditLogSink.events.single().principal)
    }

    @Test
    fun `acl allows member heartbeat but rejects monitoring reads`() {
        mockMvc.perform(
            post("/coord/v1/streams/acl-member/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("admin", "admin-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest())),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            post("/coord/v1/streams/acl-member/groups/orders-consumer/members/member-a/heartbeat")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("member", "member-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(heartbeat("member-a", memberEpoch = 0))),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value("OK"))

        mockMvc.perform(
            get("/coord/v1/monitoring/groups")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("member", "member-password")),
        )
            .andExpect(status().isForbidden)

        assertEquals(
            listOf(
                CoordinatorAuditAction.CREATE_GROUP to "SUCCESS",
            ),
            auditLogSink.events.map { it.action to it.outcome },
        )
    }

    @Test
    fun `audit log captures successful admin scale request`() {
        mockMvc.perform(
            post("/coord/v1/streams/audit-scale/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("admin", "admin-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 2))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            post("/coord/v1/streams/audit-scale/groups/orders-consumer/scale")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("admin", "admin-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"targetShardCount":4,"requestedBy":"test","reason":"audit scale"}"""),
        )
            .andExpect(status().isAccepted)

        assertEquals(
            listOf(
                CoordinatorAuditAction.CREATE_GROUP to "SUCCESS",
                CoordinatorAuditAction.SCALE_GROUP to "SUCCESS",
            ),
            auditLogSink.events.map { it.action to it.outcome },
        )
        assertEquals("audit-scale", auditLogSink.events.last().streamPrefix)
        assertEquals("orders-consumer", auditLogSink.events.last().consumerGroup)
        assertEquals("admin", auditLogSink.events.last().principal)
    }

    private fun basicAuth(username: String, password: String): String {
        val token = "$username:$password".toByteArray(StandardCharsets.UTF_8)
        return "Basic ${Base64.getEncoder().encodeToString(token)}"
    }

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            hashAlgorithm = "murmur3",
            requestedBy = "test",
        )

    private fun heartbeat(memberId: String, memberEpoch: Long): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            rebalanceTimeoutMs = 60_000,
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
        )

    @TestConfiguration
    class TestConfig {
        @Bean
        @Primary
        fun auditLogSink(): RecordingAuditLogSink =
            RecordingAuditLogSink()
    }
}

class RecordingAuditLogSink : CoordinatorAuditLogSink {
    val events = CopyOnWriteArrayList<CoordinatorAuditEvent>()

    override fun append(event: CoordinatorAuditEvent) {
        events += event
    }

    fun clear() {
        events.clear()
    }
}
