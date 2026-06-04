package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.hamcrest.Matchers.containsString
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.content
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
        "coordinator.defaults.initial-shard-count=4",
        "coordinator.defaults.consumer-max-concurrency=4",
        "coordinator.api.admin-username=admin",
        "coordinator.api.admin-password=password",
        "coordinator.api.authenticate-member-api=false",
    ],
)
class CoordinatorHttpIntegrationTest {
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
    fun `admin api requires basic auth`() {
        mockMvc.perform(get("/coord/v1/monitoring/groups"))
            .andExpect(status().isUnauthorized)
            .andExpect(header().string(HttpHeaders.WWW_AUTHENTICATE, """Basic realm="redis-stream-coordinator""""))
    }

    @Test
    fun `openapi docs and swagger ui are exposed for interactive tests`() {
        mockMvc.perform(get("/v3/api-docs"))
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.info.title").value("Redis Stream Coordinator API"))
            .andExpect(jsonPath("$.components.securitySchemes.basicAuth.type").value("http"))
            .andExpect(jsonPath("$.components.securitySchemes.basicAuth.scheme").value("basic"))
            .andExpect(jsonPath("$.components.securitySchemes.bearerAuth.type").value("http"))
            .andExpect(jsonPath("$.components.securitySchemes.bearerAuth.scheme").value("bearer"))

        mockMvc.perform(get("/swagger-ui.html"))
            .andExpect(status().is3xxRedirection)
    }

    @Test
    fun `monitoring console is exposed without triggering api basic auth`() {
        mockMvc.perform(get("/console"))
            .andExpect(status().isOk)

        mockMvc.perform(get("/console/index.html"))
            .andExpect(status().isOk)
            .andExpect(content().string(containsString("Redis Stream Coordinator")))
            .andExpect(content().string(containsString("/console/app.js")))

        mockMvc.perform(get("/console/app.js"))
            .andExpect(status().isOk)
            .andExpect(content().string(containsString("redisStreamCoordinator.console.auth")))
    }

    @Test
    fun `monitoring session verifies credentials without reading coordinator state`() {
        mockMvc.perform(get("/coord/v1/monitoring/session"))
            .andExpect(status().isUnauthorized)

        mockMvc.perform(
            get("/coord/v1/monitoring/session")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.authenticated").value(true))
            .andExpect(jsonPath("$.username").value("admin"))
    }

    @Test
    fun `login issues seven day bearer token for coordinator api calls`() {
        val loginResult = mockMvc.perform(
            post("/coord/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"username":"admin","password":"password"}"""),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.tokenType").value("Bearer"))
            .andExpect(jsonPath("$.expiresInSeconds").value(604800))
            .andExpect(jsonPath("$.roles[0]").value("ADMIN"))
            .andReturn()

        val accessToken = objectMapper.readTree(loginResult.response.contentAsString)
            .get("accessToken")
            .asString()

        mockMvc.perform(
            get("/coord/v1/monitoring/session")
                .header(HttpHeaders.AUTHORIZATION, "Bearer $accessToken"),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.authenticated").value(true))
            .andExpect(jsonPath("$.username").value("admin"))
            .andExpect(jsonPath("$.roles").isArray)

        mockMvc.perform(
            post("/coord/v1/streams/token-auth/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, "Bearer $accessToken")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 2))),
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.streamPrefix").value("token-auth"))
    }

    @Test
    fun `login rejects invalid credentials`() {
        mockMvc.perform(
            post("/coord/v1/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"username":"admin","password":"wrong"}"""),
        )
            .andExpect(status().isUnauthorized)
    }

    @Test
    fun `basic auth scheme is parsed case insensitively`() {
        mockMvc.perform(
            get("/coord/v1/monitoring/groups")
                .header(HttpHeaders.AUTHORIZATION, basicAuth().replaceFirst("Basic ", "basic ")),
        )
            .andExpect(status().isOk)
    }

    @Test
    fun `monitoring exposes coordination version compatibility lifecycle`() {
        mockMvc.perform(
            get("/coord/v1/monitoring/compatibility")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.currentCoordinationVersion").value(1))
            .andExpect(jsonPath("$.supportedCoordinationVersions.min").value(1))
            .andExpect(jsonPath("$.supportedCoordinationVersions.max").value(1))
            .andExpect(jsonPath("$.coordinationVersions[0].version").value(1))
            .andExpect(jsonPath("$.coordinationVersions[0].status").value("ACTIVE"))
            .andExpect(jsonPath("$.coordinationVersions[0].introducedIn.major").value(0))
            .andExpect(jsonPath("$.coordinationVersions[0].introducedIn.minor").value(1))
            .andExpect(jsonPath("$.coordinationVersions[0].introducedIn.patch").value(0))
            .andExpect(jsonPath("$.coordinationVersions[0].minimumSupportedUntil.major").value(1))
            .andExpect(jsonPath("$.coordinationVersions[0].minimumSupportedUntil.minor").value(0))
            .andExpect(jsonPath("$.coordinationVersions[0].minimumSupportedUntil.patch").value(0))
    }

    @Test
    fun `admin api validates create group body`() {
        mockMvc.perform(
            post("/coord/v1/streams/http-validation/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content("""{"initialShardCount":0,"requestedBy":""}"""),
        )
            .andExpect(status().isBadRequest)
            .andExpect(jsonPath("$.errorCode").value(CoordinatorError.INVALID_REQUEST.code))
    }

    @Test
    fun `http api creates group and accepts heartbeat`() {
        mockMvc.perform(
            post("/coord/v1/streams/http-orders/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 2))),
        )
            .andExpect(status().isCreated)
            .andExpect(jsonPath("$.streamPrefix").value("http-orders"))
            .andExpect(jsonPath("$.consumerGroup").value("orders-consumer"))
            .andExpect(jsonPath("$.shardCount").value(2))

        mockMvc.perform(
            post("/coord/v1/streams/http-orders/groups/orders-consumer/members/member-a/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(heartbeat("member-a", memberEpoch = 0))),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value("OK"))
            .andExpect(jsonPath("$.assignment.assignedShards.length()").value(2))

        mockMvc.perform(
            get("/coord/v1/monitoring/streams/http-orders/groups/orders-consumer/assignments")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.targetAssignment['member-a'].length()").value(2))
    }

    @Test
    fun `http api returns producer routing metadata`() {
        mockMvc.perform(
            post("/coord/v1/streams/http-routing/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 2))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            get("/coord/v1/streams/http-routing/groups/orders-consumer/producer-routing")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.streamPrefix").value("http-routing"))
            .andExpect(jsonPath("$.consumerGroup").value("orders-consumer"))
            .andExpect(jsonPath("$.metadataVersion").value(1))
                        .andExpect(jsonPath("$.shardCount").value(2))
            .andExpect(jsonPath("$.streamKeyPattern").value("http-routing:{shardIndex}"))
            .andExpect(jsonPath("$.shards.length()").value(2))
            .andExpect(jsonPath("$.shards[0].streamKey").value("http-routing:0"))
            .andExpect(jsonPath("$.shards[1].streamKey").value("http-routing:1"))
    }

    @Test
    fun `http api scales group and refreshes producer routing metadata`() {
        mockMvc.perform(
            post("/coord/v1/streams/http-scale-routing/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 2))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            post("/coord/v1/streams/http-scale-routing/groups/orders-consumer/scale")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    objectMapper.writeValueAsString(
                        ScaleGroupRequest(
                            targetShardCount = 4,
                            requestedBy = "test",
                            reason = "http scale routing",
                        ),
                    ),
                ),
        )
            .andExpect(status().isAccepted)
            .andExpect(jsonPath("$.reshardingId").exists())
                                    .andExpect(jsonPath("$.toShardCount").value(4))

        mockMvc.perform(
            get("/coord/v1/streams/http-scale-routing/groups/orders-consumer/producer-routing")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.metadataVersion").value(2))
                        .andExpect(jsonPath("$.shardCount").value(4))
            .andExpect(jsonPath("$.shards.length()").value(4))
            .andExpect(jsonPath("$.shards[0].streamKey").value("http-scale-routing:0"))
            .andExpect(jsonPath("$.shards[3].streamKey").value("http-scale-routing:3"))

        mockMvc.perform(
            get("/coord/v1/monitoring/streams/http-scale-routing/groups/orders-consumer/migrations")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.migrations.length()").value(1))
            .andExpect(jsonPath("$.migrations[0].reshardingId").exists())
            .andExpect(jsonPath("$.activeReshardingId").exists())
    }

    @Test
    fun `http api handles graceful leave and monitoring shows empty group`() {
        mockMvc.perform(
            post("/coord/v1/streams/http-leave-empty/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 1))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            post("/coord/v1/streams/http-leave-empty/groups/orders-consumer/members/member-a/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(heartbeat("member-a", memberEpoch = 0))),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value("OK"))

        mockMvc.perform(
            post("/coord/v1/streams/http-leave-empty/groups/orders-consumer/members/member-a/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content(
                    objectMapper.writeValueAsString(
                        heartbeat(
                            "member-a",
                            memberEpoch = -1,
                            ownedShards = setOf(ShardId(0)),
                        ),
                    ),
                ),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.status").value("OK"))

        mockMvc.perform(
            get("/coord/v1/monitoring/streams/http-leave-empty/groups/orders-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth()),
        )
            .andExpect(status().isOk)
            .andExpect(jsonPath("$.state").value("EMPTY"))
            .andExpect(jsonPath("$.targetAssignmentSummary").isEmpty)
    }

    @Test
    fun `member api allows heartbeat without auth by default`() {
        mockMvc.perform(
            post("/coord/v1/streams/http-auth/groups/auth-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 1))),
        )
            .andExpect(status().isCreated)

        mockMvc.perform(
            post("/coord/v1/streams/http-auth/groups/auth-consumer/members/member-a/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(heartbeat("member-a", memberEpoch = 0))),
        )
            .andExpect(status().isOk)
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

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
    ): HeartbeatRequest =
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
            ownedShards = ownedShards,
        )
}

@SpringBootTest(
    properties = [
        "coordinator.store.type=memory",
        "coordinator.api.admin-username=admin",
        "coordinator.api.admin-password=password",
        "coordinator.api.authenticate-member-api=true",
    ],
)
class CoordinatorAuthenticatedMemberHttpIntegrationTest {
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
    fun `member api requires basic auth when enabled`() {
        mockMvc.perform(
            post("/coord/v1/streams/http-member-auth/groups/auth-consumer")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(createGroupRequest(initialShardCount = 1))),
        )
            .andExpect(status().isCreated)

        val heartbeatBody = objectMapper.writeValueAsString(heartbeat("member-a", memberEpoch = 0))

        mockMvc.perform(
            post("/coord/v1/streams/http-member-auth/groups/auth-consumer/members/member-a/heartbeat")
                .contentType(MediaType.APPLICATION_JSON)
                .content(heartbeatBody),
        )
            .andExpect(status().isUnauthorized)

        mockMvc.perform(
            post("/coord/v1/streams/http-member-auth/groups/auth-consumer/members/member-a/heartbeat")
                .header(HttpHeaders.AUTHORIZATION, basicAuth())
                .contentType(MediaType.APPLICATION_JSON)
                .content(heartbeatBody),
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
}
