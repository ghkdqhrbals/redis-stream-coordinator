package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

@DisplayName("Coordinator grouped workflows")
class CoordinatorGroupedWorkflowTest {
    @Nested
    @DisplayName("Member expired")
    inner class MemberExpired {
        @Test
        fun `expired owner is reassigned and stale heartbeat is fenced`() {
            val clock = CategoryClock()
            val service = service(clock)
            service.createGroup("category-expired", "orders-consumer", createGroupRequest(initialShardCount = 2))
            val memberA = join(service, "category-expired", "member-a")
            acknowledge(service, "category-expired", "member-a", memberA)

            clock.advance(Duration.ofSeconds(16))
            val memberB = join(service, "category-expired", "member-b")
            val staleA = service.heartbeat(
                "category-expired",
                "orders-consumer",
                "member-a",
                heartbeat("member-a", memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
            )
            val assignments = service.assignments("category-expired", "orders-consumer")

            assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, staleA.status)
            assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), memberB.assignment.assignedShards)
            assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.targetAssignment.getValue("member-b"))
            assertTrue(assignments.invariantViolations.isEmpty())
        }

        @Test
        fun `expired member rejoin ignores stale ownership report`() {
            val clock = CategoryClock()
            val service = service(clock)
            service.createGroup("category-expired-rejoin", "orders-consumer", createGroupRequest(initialShardCount = 2))
            val memberA = join(service, "category-expired-rejoin", "member-a")
            acknowledge(service, "category-expired-rejoin", "member-a", memberA)
            clock.advance(Duration.ofSeconds(16))
            val memberB = join(service, "category-expired-rejoin", "member-b")
            acknowledge(service, "category-expired-rejoin", "member-b", memberB)

            val rejoinedA = service.heartbeat(
                "category-expired-rejoin",
                "orders-consumer",
                "member-a",
                heartbeat("member-a", memberEpoch = 0, ownedShards = memberA.assignment.assignedShards),
            )
            val assignments = service.assignments("category-expired-rejoin", "orders-consumer")

            assertEquals(HeartbeatStatus.OK, rejoinedA.status)
            assertTrue(rejoinedA.assignment.assignedShards.isEmpty())
            assertEquals(emptySet(), assignments.currentAssignments.getValue("member-a"))
            assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.currentAssignments.getValue("member-b"))
            assertTrue(assignments.invariantViolations.isEmpty())
        }
    }

    @Nested
    @DisplayName("Member join")
    inner class MemberJoin {
        @Test
        fun `first member receives every initial shard`() {
            val service = service()
            service.createGroup("category-join-first", "orders-consumer", createGroupRequest(initialShardCount = 3))

            val joined = join(service, "category-join-first", "member-a")

            assertEquals(setOf(ShardId(1, 0), ShardId(1, 1), ShardId(1, 2)), joined.assignment.assignedShards)
            assertTrue(joined.assignment.pendingShards.isEmpty())
        }

        @Test
        fun `new member waits for previous owner revoke before receiving moved shard`() {
            val service = service()
            service.createGroup("category-join-second", "orders-consumer", createGroupRequest(initialShardCount = 2))
            val memberA = join(service, "category-join-second", "member-a")
            acknowledge(service, "category-join-second", "member-a", memberA)

            val memberB = join(service, "category-join-second", "member-b")
            assertEquals(setOf(ShardId(1, 1)), memberB.assignment.pendingShards)

            service.heartbeat(
                "category-join-second",
                "orders-consumer",
                "member-a",
                heartbeat(
                    "member-a",
                    memberA.memberEpoch,
                    ownedShards = setOf(ShardId(1, 0)),
                    revokingShards = listOf(RevokingShardReport(ShardId(1, 1), RevokingShardState.REVOKED)),
                ),
            )
            val assignedToB = service.heartbeat(
                "category-join-second",
                "orders-consumer",
                "member-b",
                heartbeat("member-b", memberB.memberEpoch),
            )

            assertEquals(setOf(ShardId(1, 1)), assignedToB.assignment.assignedShards)
            assertTrue(assignedToB.assignment.pendingShards.isEmpty())
        }

        @Test
        fun `three member join creates balanced target assignment without duplicate owners`() {
            val service = service()
            service.createGroup("category-join-three", "orders-consumer", createGroupRequest(initialShardCount = 6))
            join(service, "category-join-three", "member-a")
            join(service, "category-join-three", "member-b")
            join(service, "category-join-three", "member-c")

            val assignments = service.assignments("category-join-three", "orders-consumer")
            val targetShards = assignments.targetAssignment.values.flatten()

            assertEquals(mapOf("member-a" to 2, "member-b" to 2, "member-c" to 2), assignments.targetAssignment.mapValues { it.value.size })
            assertEquals(6, targetShards.toSet().size)
            assertEquals((0 until 6).map { ShardId(1, it) }.toSet(), targetShards.toSet())
            assertTrue(assignments.invariantViolations.isEmpty())
        }
    }

    @Nested
    @DisplayName("Graceful leave")
    inner class GracefulLeave {
        @Test
        fun `leaving member releases targets and survivor owns every shard`() {
            val service = service()
            service.createGroup("category-leave", "orders-consumer", createGroupRequest(initialShardCount = 2))
            val memberA = join(service, "category-leave", "member-a")
            acknowledge(service, "category-leave", "member-a", memberA)
            val memberB = join(service, "category-leave", "member-b")

            service.heartbeat(
                "category-leave",
                "orders-consumer",
                "member-a",
                heartbeat("member-a", memberEpoch = -1, ownedShards = memberA.assignment.assignedShards),
            )
            val survivor = service.heartbeat(
                "category-leave",
                "orders-consumer",
                "member-b",
                heartbeat("member-b", memberB.memberEpoch, ownedShards = memberB.assignment.assignedShards),
            )
            val assignments = service.assignments("category-leave", "orders-consumer")

            assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), survivor.assignment.assignedShards)
            assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), assignments.targetAssignment.getValue("member-b"))
            assertTrue("member-a" !in assignments.targetAssignment)
            assertTrue(assignments.invariantViolations.isEmpty())
        }

        @Test
        fun `last member leave makes group empty and clears targets`() {
            val service = service()
            service.createGroup("category-leave-empty", "orders-consumer", createGroupRequest(initialShardCount = 2))
            val memberA = join(service, "category-leave-empty", "member-a")
            acknowledge(service, "category-leave-empty", "member-a", memberA)

            val leave = service.heartbeat(
                "category-leave-empty",
                "orders-consumer",
                "member-a",
                heartbeat("member-a", memberEpoch = -1, ownedShards = memberA.assignment.assignedShards),
            )
            val group = service.getGroup("category-leave-empty", "orders-consumer")
            val assignments = service.assignments("category-leave-empty", "orders-consumer")

            assertEquals(HeartbeatStatus.OK, leave.status)
            assertEquals(GroupState.EMPTY, group.state)
            assertTrue(assignments.targetAssignment.isEmpty())
            assertTrue(assignments.invariantViolations.isEmpty())
        }
    }

    @Nested
    @DisplayName("Partition upscaling")
    inner class PartitionUpscaling {
        @Test
        fun `scale creates next stream version and assigns old plus new shards`() {
            val service = service()
            service.createGroup("category-upscale", "orders-consumer", createGroupRequest(initialShardCount = 2))
            val memberA = join(service, "category-upscale", "member-a")
            acknowledge(service, "category-upscale", "member-a", memberA)

            val migration = service.scaleGroup(
                "category-upscale",
                "orders-consumer",
                ScaleGroupRequest(targetShardCount = 4, requestedBy = "test", reason = "category upscale"),
            )
            val heartbeat = service.heartbeat(
                "category-upscale",
                "orders-consumer",
                "member-a",
                heartbeat("member-a", memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
            )
            val group = service.getGroup("category-upscale", "orders-consumer")

            assertEquals(1, migration.fromVersion)
            assertEquals(2, migration.toVersion)
            assertEquals(setOf(1, 2), group.readableVersions)
            assertEquals((0 until 2).map { ShardId(1, it) }.toSet() + (0 until 4).map { ShardId(2, it) }.toSet(), heartbeat.assignment.assignedShards)
        }

        @Test
        fun `scale updates producer routing while keeping old version readable`() {
            val service = service()
            service.createGroup("category-upscale-routing", "orders-consumer", createGroupRequest(initialShardCount = 2))
            val before = service.producerRouting("category-upscale-routing", "orders-consumer")

            service.scaleGroup(
                "category-upscale-routing",
                "orders-consumer",
                ScaleGroupRequest(targetShardCount = 5, requestedBy = "test", reason = "route new version"),
            )
            val after = service.producerRouting("category-upscale-routing", "orders-consumer")
            val group = service.getGroup("category-upscale-routing", "orders-consumer")

            assertEquals(1, before.activeWriteVersion)
            assertEquals(2, after.activeWriteVersion)
            assertEquals(5, after.shardCount)
            assertEquals(setOf(1, 2), group.readableVersions)
            assertEquals((0 until 5).map { "category-upscale-routing:v2:shard:$it" }, after.shards.map { it.streamKey })
            assertTrue(after.metadataVersion > before.metadataVersion)
        }
    }

    @Nested
    @DisplayName("New stream topic creation")
    inner class NewStreamTopicCreation {
        @Test
        fun `group creation provisions initial stream topic shards`() {
            val provisioner = CategoryRecordingStreamShardProvisioner()
            val service = service(streamProvisioner = provisioner)

            service.createGroup("category-topic-create", "orders-consumer", createGroupRequest(initialShardCount = 3))

            assertEquals(
                listOf(CategoryProvisionedVersion("category-topic-create", "orders-consumer", 1, 3)),
                provisioner.provisioned,
            )
            assertEquals(
                listOf("category-topic-create:v1:shard:0", "category-topic-create:v1:shard:1", "category-topic-create:v1:shard:2"),
                provisioner.plans.single().shardKeys.map { it.value },
            )
        }

        @Test
        fun `upscaling provisions next stream topic shards after preparing migration`() {
            val provisioner = CategoryRecordingStreamShardProvisioner()
            val service = service(streamProvisioner = provisioner)
            service.createGroup("category-topic-scale", "orders-consumer", createGroupRequest(initialShardCount = 2))

            val migration = service.scaleGroup(
                "category-topic-scale",
                "orders-consumer",
                ScaleGroupRequest(targetShardCount = 5, requestedBy = "test", reason = "topic upscale"),
            )

            assertEquals(2, migration.toVersion)
            assertEquals(
                listOf(
                    CategoryProvisionedVersion("category-topic-scale", "orders-consumer", 1, 2),
                    CategoryProvisionedVersion("category-topic-scale", "orders-consumer", 2, 5),
                ),
                provisioner.provisioned,
            )
        }

        @Test
        fun `same shard count scale does not provision a new stream topic version`() {
            val provisioner = CategoryRecordingStreamShardProvisioner()
            val service = service(streamProvisioner = provisioner)
            service.createGroup("category-topic-noop", "orders-consumer", createGroupRequest(initialShardCount = 3))

            val migration = service.scaleGroup(
                "category-topic-noop",
                "orders-consumer",
                ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "noop"),
            )
            val group = service.getGroup("category-topic-noop", "orders-consumer")

            assertEquals(MigrationState.DEPRECATED, migration.state)
            assertEquals(1, group.activeWriteVersion)
            assertEquals(null, group.activeMigration)
            assertEquals(listOf(CategoryProvisionedVersion("category-topic-noop", "orders-consumer", 1, 3)), provisioner.provisioned)
        }
    }

    private fun service(
        clock: Clock = CategoryClock(),
        streamProvisioner: StreamShardProvisioner = NoopStreamShardProvisioner,
    ): CoordinatorService =
        CoordinatorService(
            properties = CoordinatorProperties(
                heartbeatInterval = Duration.ofSeconds(3),
                memberLeaseTtl = Duration.ofSeconds(15),
                defaults = CoordinatorProperties.Defaults(
                    initialShardCount = 4,
                    consumerMaxConcurrency = 4,
                ),
            ),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = streamProvisioner,
            clock = clock,
        )

    private fun createGroupRequest(initialShardCount: Int): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun join(service: CoordinatorService, streamPrefix: String, memberId: String): HeartbeatResponse =
        service.heartbeat(streamPrefix, "orders-consumer", memberId, heartbeat(memberId, memberEpoch = 0))

    private fun acknowledge(
        service: CoordinatorService,
        streamPrefix: String,
        memberId: String,
        response: HeartbeatResponse,
    ): HeartbeatResponse =
        service.heartbeat(
            streamPrefix,
            "orders-consumer",
            memberId,
            heartbeat(memberId, response.memberEpoch, ownedShards = response.assignment.assignedShards),
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
        revokingShards: List<RevokingShardReport> = emptyList(),
    ): HeartbeatRequest =
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
            ownedShards = ownedShards,
            revokingShards = revokingShards,
        )
}

private data class CategoryProvisionedVersion(
    val streamPrefix: String,
    val consumerGroup: String,
    val streamVersion: Int,
    val shardCount: Int,
)

private class CategoryRecordingStreamShardProvisioner : StreamShardProvisioner {
    val plans = mutableListOf<RedisStreamShardProvisioningPlan>()
    val provisioned = mutableListOf<CategoryProvisionedVersion>()

    override fun provision(plan: RedisStreamShardProvisioningPlan) {
        plans += plan
        provisioned += CategoryProvisionedVersion(
            streamPrefix = plan.streamPrefix,
            consumerGroup = plan.consumerGroup,
            streamVersion = plan.streamVersion,
            shardCount = plan.shardCount,
        )
    }
}

private class CategoryClock(
    private var current: Instant = Instant.parse("2026-05-21T00:00:00Z"),
    private val zone: ZoneId = ZoneOffset.UTC,
) : Clock() {
    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock =
        CategoryClock(current, zone)

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
        current = current.plus(duration)
    }
}
