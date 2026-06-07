package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditAction
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditEvent
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditLogSink
import io.github.ghkdqhrbals.redisstreamcoordinator.config.audit.AuditRequestCachingFilter
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
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
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
        "coordinator.api.users[0].username=writer",
        "coordinator.api.users[0].password=write-password",
        "coordinator.api.users[0].roles[0]=WRITE",
        "coordinator.api.users[1].username=reader",
        "coordinator.api.users[1].password=read-password",
        "coordinator.api.users[1].roles[0]=READ",
        "coordinator.api.users[2].username=member",
        "coordinator.api.users[2].password=member-password",
        "coordinator.api.users[2].roles[0]=MEMBER",
        "coordinator.api.users[3].username=legacy-admin",
        "coordinator.api.users[3].password=admin-password",
        "coordinator.api.users[3].roles[0]=ADMIN",
        "coordinator.api.users[4].username=legacy-monitor",
        "coordinator.api.users[4].password=monitor-password",
        "coordinator.api.users[4].roles[0]=MONITOR",
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
        val builder = MockMvcBuilders.webAppContextSetup(webApplicationContext)
        builder.addFilters<DefaultMockMvcBuilder>(AuditRequestCachingFilter())
        mockMvc = builder.build()
    }

    @Test
    fun `acl allows read role monitoring reads but rejects admin mutations`() {
        mockMvc.perform(
            get("/coord/v1/monitoring/groups")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("reader", "read-password")),
        )
            .andExpect(status().isOk)

        mockMvc.perform(
            post("/coord/v1/streams/acl-denied/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("reader", "read-password"))
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
        assertEquals("reader", auditLogSink.events.single().principal)
    }

    @Test
    fun `acl allows write role to read monitoring and mutate admin endpoints`() {
        mockMvc.perform(
            get("/coord/v1/monitoring/groups")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password")),
        )
            .andExpect(status().isOk)

        mockMvc.perform(
            post("/coord/v1/streams/acl-write/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest())),
        )
            .andExpect(status().isCreated)

        assertEquals(
            listOf(CoordinatorAuditAction.CREATE_GROUP to "SUCCESS"),
            auditLogSink.events.map { it.action to it.outcome },
        )
        assertEquals("writer", auditLogSink.events.single().principal)
    }

    @Test
    fun `acl allows read role on read only stream endpoints`() {
        mockMvc.perform(
            post("/coord/v1/streams/acl-read-stream/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 2))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            get("/coord/v1/streams/acl-read-stream/groups/orders-consumer/producer-routing")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("reader", "read-password")),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.streamPrefix").value("acl-read-stream"))
            .andExpect(jsonPath("$.consumerGroup").value("orders-consumer"))

        assertEquals(
            listOf(CoordinatorAuditAction.CREATE_GROUP to "SUCCESS"),
            auditLogSink.events.map { it.action to it.outcome },
        )
    }

    @Test
    fun `acl keeps legacy admin and monitor roles as aliases`() {
        mockMvc.perform(
            get("/coord/v1/monitoring/groups")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("legacy-monitor", "monitor-password")),
        )
            .andExpect(status().isOk)

        mockMvc.perform(
            post("/coord/v1/streams/acl-legacy/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("legacy-admin", "admin-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest())),
        )
            .andExpect(status().isCreated)
    }

    @Test
    fun `acl allows member heartbeat but rejects monitoring reads`() {
        mockMvc.perform(
            post("/coord/v1/streams/acl-member/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password"))
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
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 2))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            post("/coord/v1/streams/audit-scale/groups/orders-consumer/scale")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password"))
                .header("X-Request-Id", "audit-scale-request")
                .header("X-Forwarded-For", "203.0.113.10")
                .header(HttpHeaders.USER_AGENT, "audit-test")
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
        assertEquals("writer", auditLogSink.events.last().principal)
        assertEquals("test", auditLogSink.events.last().requestedBy)
        assertEquals("audit scale", auditLogSink.events.last().reason)
        assertEquals("audit-scale-request", auditLogSink.events.last().requestId)
        assertEquals("203.0.113.10", auditLogSink.events.last().clientAddress)
        assertEquals("audit-test", auditLogSink.events.last().userAgent)
        assertEquals(listOf("WRITE"), auditLogSink.events.last().roles)
        assertEquals("4", auditLogSink.events.last().requestSummary["targetShardCount"])
        requireNotNull(auditLogSink.events.last().requestBodySha256)
        requireNotNull(auditLogSink.events.last().durationMs)
    }

    @Test
    fun `audit log captures delete group mutation`() {
        mockMvc.perform(
            post("/coord/v1/streams/audit-delete/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 1))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            delete("/coord/v1/streams/audit-delete/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth("writer", "write-password"))
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"requestedBy":"test","reason":"cleanup","force":false}"""),
        )
            .andExpect(status().isOk)

        assertEquals(
            listOf(
                CoordinatorAuditAction.CREATE_GROUP to "SUCCESS",
                CoordinatorAuditAction.DELETE_GROUP to "SUCCESS",
            ),
            auditLogSink.events.map { it.action to it.outcome },
        )
        assertEquals("cleanup", auditLogSink.events.last().reason)
        assertEquals("false", auditLogSink.events.last().requestSummary["force"])
    }

    private fun basicAuth(username: String, password: String): String {
        val token = "$username:$password".toByteArray(StandardCharsets.UTF_8)
        return "Basic ${Base64.getEncoder().encodeToString(token)}"
    }

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun heartbeat(memberId: String, memberEpoch: Long): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
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
