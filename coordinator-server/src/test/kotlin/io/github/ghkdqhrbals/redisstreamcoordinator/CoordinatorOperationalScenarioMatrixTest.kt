package io.github.ghkdqhrbals.redisstreamcoordinator

import org.junit.jupiter.api.Assertions.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.stream.Stream
import kotlin.math.max
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CoordinatorOperationalScenarioMatrixTest {
    @ParameterizedTest(name = "{0}")
    @MethodSource("noScaleScenarios")
    fun `matrix no scale preserves invariants`(scenario: OperationalScenario) {
        runScenario(scenario)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scaleUpScenarios")
    fun `matrix scale up preserves invariants`(scenario: OperationalScenario) {
        runScenario(scenario)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scaleDownScenarios")
    fun `matrix scale down preserves invariants`(scenario: OperationalScenario) {
        runScenario(scenario)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rollbackScenarios")
    fun `matrix rollback preserves invariants`(scenario: OperationalScenario) {
        runScenario(scenario)
    }

    private fun runScenario(scenario: OperationalScenario) {
        val clock = ScenarioClock(Instant.parse("2026-05-21T00:00:00Z"), ZoneOffset.UTC)
        val service = service(clock)
        val runtime = ScenarioRuntime(scenario, service, clock)

        runtime.createGroup()
        runtime.joinInitialMembers()
        runtime.converge()
        runtime.applyConcurrencyMode()
        runtime.converge()
        runtime.applyScaleMode()
        runtime.converge()
        runtime.applyChurnMode()
        runtime.converge()
        runtime.assertOperationalInvariants()
    }

    companion object {
        @JvmStatic
        fun noScaleScenarios(): Stream<Arguments> =
            operationalScenarios(ScaleMode.NONE)

        @JvmStatic
        fun scaleUpScenarios(): Stream<Arguments> =
            operationalScenarios(ScaleMode.SCALE_UP)

        @JvmStatic
        fun scaleDownScenarios(): Stream<Arguments> =
            operationalScenarios(ScaleMode.SCALE_DOWN)

        @JvmStatic
        fun rollbackScenarios(): Stream<Arguments> =
            operationalScenarios(ScaleMode.SCALE_AND_ROLLBACK)

        private fun operationalScenarios(scaleMode: ScaleMode): Stream<Arguments> {
            val shardCounts = listOf(2, 6, 12)
            val memberCounts = listOf(2, 3, 5)
            return ChurnMode.entries.flatMap { churnMode ->
                ConcurrencyMode.entries.flatMap { concurrencyMode ->
                    shardCounts.flatMap { shardCount ->
                        memberCounts.map { memberCount ->
                            Arguments.of(
                                OperationalScenario(
                                    initialShardCount = shardCount,
                                    initialMemberCount = memberCount,
                                    scaleMode = scaleMode,
                                    churnMode = churnMode,
                                    concurrencyMode = concurrencyMode,
                                ),
                            )
                        }
                    }
                }
            }.stream()
        }
    }
}

private class ScenarioRuntime(
    private val scenario: OperationalScenario,
    private val service: CoordinatorService,
    private val clock: ScenarioClock,
) {
    private val streamPrefix = "ops-${scenario.id}"
    private val consumerGroup = "ops-consumer"
    private val memberEpochs = linkedMapOf<String, Long>()
    private val ownedShards = linkedMapOf<String, Set<ShardId>>()
    private var nextMemberIndex = scenario.initialMemberCount
    private var scaleTargetShardCount: Int? = null
    private var rolledBack = false

    fun createGroup() {
        service.createGroup(
            streamPrefix,
            consumerGroup,
            CreateGroupRequest(
                initialShardCount = scenario.initialShardCount,
                hashAlgorithm = "murmur3",
                requestedBy = "ops-test",
                reason = scenario.toString(),
            ),
        )
    }

    fun joinInitialMembers() {
        repeat(scenario.initialMemberCount) { index ->
            joinMember(memberId(index))
        }
    }

    fun applyConcurrencyMode() {
        if (scenario.concurrencyMode == ConcurrencyMode.UNIFORM || liveMembers().isEmpty()) {
            return
        }
        service.updateConsumerConcurrency(
            streamPrefix,
            consumerGroup,
            UpdateConsumerConcurrencyRequest(
                defaultMaxConcurrency = 1,
                memberOverrides = weightedOverrides(),
                requestedBy = "ops-test",
                reason = "capacity rebalance for ${scenario.concurrencyMode.label}",
            ),
        )
    }

    fun applyScaleMode() {
        when (scenario.scaleMode) {
            ScaleMode.NONE -> return
            ScaleMode.SCALE_UP -> scaleTo(scenario.initialShardCount + max(1, scenario.initialShardCount / 2))
            ScaleMode.SCALE_DOWN -> scaleTo(max(1, scenario.initialShardCount / 2))
            ScaleMode.SCALE_AND_ROLLBACK -> {
                val migration = scaleTo(scenario.initialShardCount + max(1, scenario.initialShardCount / 2))
                service.rollbackMigration(streamPrefix, consumerGroup, migration.migrationId)
                scaleTargetShardCount = null
                rolledBack = true
            }
        }
    }

    fun applyChurnMode() {
        when (scenario.churnMode) {
            ChurnMode.STEADY -> return
            ChurnMode.ADD_MEMBER -> joinMember(nextMember())
            ChurnMode.GRACEFUL_LEAVE -> gracefullyLeave(memberToRemove())
            ChurnMode.EXPIRE_MEMBER -> expireMember(memberToRemove())
            ChurnMode.ROLLING_RESTART -> {
                val restarting = memberToRemove()
                gracefullyLeave(restarting)
                converge()
                joinMember(restarting)
            }
            ChurnMode.REPLACE_MEMBER -> {
                gracefullyLeave(memberToRemove())
                joinMember(nextMember())
            }
        }
    }

    fun converge(maxRounds: Int = 12) {
        repeat(maxRounds) {
            liveMembers().forEach { memberId ->
                val response = service.heartbeat(
                    streamPrefix,
                    consumerGroup,
                    memberId,
                    heartbeat(memberId, memberEpochs.getValue(memberId), ownedShards[memberId].orEmpty()),
                )
                assertEquals(HeartbeatStatus.OK, response.status, "$scenario heartbeat failed for $memberId")
                memberEpochs[memberId] = response.memberEpoch
                ownedShards[memberId] = response.assignment.assignedShards
            }
            if (service.assignments(streamPrefix, consumerGroup).isConverged(liveMembers())) {
                return
            }
        }
    }

    fun assertOperationalInvariants() {
        val group = service.getGroup(streamPrefix, consumerGroup)
        val members = service.listMembers(streamPrefix, consumerGroup).members
        val assignments = service.assignments(streamPrefix, consumerGroup)
        val liveMembers = members
            .filter { it.state == MemberState.ACTIVE || it.state == MemberState.STARTING }
            .map { it.memberId }
            .toSet()
        val expectedReadableShards = expectedReadableShards()
        val targetEntries = assignments.targetAssignment.entries
        val targetedShards = targetEntries.flatMap { it.value }
        val duplicateTargets = targetedShards
            .groupingBy { it }
            .eachCount()
            .filterValues { it > 1 }

        assertAll(
            { assertEquals(emptyMap(), duplicateTargets, "$scenario duplicate target owners") },
            { assertTrue(assignments.invariantViolations.isEmpty(), "$scenario invariant violations ${assignments.invariantViolations}") },
            { assertEquals(group.groupEpoch, group.assignmentEpoch, "$scenario assignment epoch must track group epoch") },
            { assertEquals(rolledBack, group.activeMigration == null && scenario.scaleMode == ScaleMode.SCALE_AND_ROLLBACK) },
            {
                if (liveMembers.isEmpty()) {
                    assertTrue(assignments.targetAssignment.isEmpty(), "$scenario should not have targets without live members")
                    assertEquals(GroupState.EMPTY, group.state, "$scenario should be empty without live members")
                } else {
                    assertEquals(expectedReadableShards, targetedShards.toSet(), "$scenario target coverage mismatch")
                    assertTrue(targetEntries.all { it.key in liveMembers }, "$scenario target references non-live members")
                    liveMembers.forEach { memberId ->
                        assertEquals(
                            assignments.targetAssignment[memberId].orEmpty(),
                            assignments.currentAssignments[memberId].orEmpty(),
                            "$scenario current assignment should converge for $memberId",
                        )
                    }
                    assertFalse(group.state == GroupState.EMPTY, "$scenario should not be empty with live members")
                }
            },
            {
                group.activeMigration?.let { migration ->
                    assertEquals(MigrationState.ACTIVE, migration.state, "$scenario active migration should be active")
                    assertEquals(2, migration.toVersion, "$scenario first scale should create version 2")
                }
            },
        )
    }

    private fun joinMember(memberId: String) {
        val response = service.heartbeat(
            streamPrefix,
            consumerGroup,
            memberId,
            heartbeat(memberId, memberEpoch = 0, ownedShards = emptySet()),
        )
        assertEquals(HeartbeatStatus.OK, response.status, "$scenario join failed for $memberId")
        memberEpochs[memberId] = response.memberEpoch
        ownedShards[memberId] = response.assignment.assignedShards
    }

    private fun gracefullyLeave(memberId: String) {
        if (memberId !in memberEpochs) {
            return
        }
        service.heartbeat(
            streamPrefix,
            consumerGroup,
            memberId,
            heartbeat(memberId, memberEpoch = -1, ownedShards = ownedShards[memberId].orEmpty()),
        )
        memberEpochs.remove(memberId)
        ownedShards.remove(memberId)
        if (memberEpochs.isEmpty()) {
            joinMember(nextMember())
        }
    }

    private fun expireMember(memberId: String) {
        val survivors = memberEpochs.keys.filterNot { it == memberId }
        clock.advance(Duration.ofSeconds(8))
        survivors.forEach { survivor ->
            val response = service.heartbeat(
                streamPrefix,
                consumerGroup,
                survivor,
                heartbeat(survivor, memberEpochs.getValue(survivor), ownedShards[survivor].orEmpty()),
            )
            assertEquals(HeartbeatStatus.OK, response.status, "$scenario survivor heartbeat failed for $survivor")
            memberEpochs[survivor] = response.memberEpoch
            ownedShards[survivor] = response.assignment.assignedShards
        }
        clock.advance(Duration.ofSeconds(8))
        service.getGroup(streamPrefix, consumerGroup)
        memberEpochs.remove(memberId)
        ownedShards.remove(memberId)
        if (memberEpochs.isEmpty()) {
            joinMember(nextMember())
        }
    }

    private fun scaleTo(targetShardCount: Int): Migration {
        val normalizedTarget = if (targetShardCount == scenario.initialShardCount) {
            scenario.initialShardCount + 1
        } else {
            targetShardCount
        }
        scaleTargetShardCount = normalizedTarget
        return service.scaleGroup(
            streamPrefix,
            consumerGroup,
            ScaleGroupRequest(
                targetShardCount = normalizedTarget,
                requestedBy = "ops-test",
                reason = "operational ${scenario.scaleMode.label}",
            ),
        )
    }

    private fun weightedOverrides(): Map<String, Int> {
        val live = liveMembers()
        if (live.isEmpty()) return emptyMap()
        val weightedMember = when (scenario.concurrencyMode) {
            ConcurrencyMode.UNIFORM -> return emptyMap()
            ConcurrencyMode.FIRST_MEMBER_HAS_HIGHER_CAPACITY -> live.first()
        }
        return mapOf(weightedMember to 4)
    }

    private fun expectedReadableShards(): Set<ShardId> {
        val activeScaleTarget = scaleTargetShardCount
        return when {
            activeScaleTarget != null && !rolledBack -> {
                (0 until scenario.initialShardCount).map { ShardId(1, it) }.toSet() +
                    (0 until activeScaleTarget).map { ShardId(2, it) }.toSet()
            }
            else -> (0 until scenario.initialShardCount).map { ShardId(1, it) }.toSet()
        }
    }

    private fun liveMembers(): List<String> =
        memberEpochs.keys.sorted()

    private fun memberToRemove(): String =
        liveMembers().firstOrNull() ?: nextMember().also { joinMember(it) }

    private fun nextMember(): String =
        memberId(nextMemberIndex++)

    private fun memberId(index: Int): String =
        "member-${index.toString().padStart(2, '0')}"
}

private fun AssignmentsResponse.isConverged(liveMembers: List<String>): Boolean =
    liveMembers.all { memberId ->
        targetAssignment[memberId].orEmpty() == currentAssignments[memberId].orEmpty()
    }

private fun service(clock: Clock): CoordinatorService =
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
        streamProvisioner = NoopStreamShardProvisioner,
        clock = clock,
    )

private fun heartbeat(
    memberId: String,
    memberEpoch: Long,
    ownedShards: Set<ShardId>,
): HeartbeatRequest =
    HeartbeatRequest(
        protocolVersion = 1,
        requestId = "hb-$memberId-$memberEpoch-${ownedShards.size}",
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
    )

data class OperationalScenario(
    val initialShardCount: Int,
    val initialMemberCount: Int,
    val scaleMode: ScaleMode,
    val churnMode: ChurnMode,
    val concurrencyMode: ConcurrencyMode,
) {
    val id: String =
        "s${initialShardCount}-m${initialMemberCount}-${scaleMode.name.lowercase()}-${churnMode.name.lowercase()}-${concurrencyMode.name.lowercase()}"

    val displayName: String =
        "${scaleMode.label} | ${churnMode.label} | ${concurrencyMode.label} | $initialShardCount shards, $initialMemberCount members"

    override fun toString(): String =
        displayName
}

enum class ScaleMode(val label: String) {
    NONE("no scale"),
    SCALE_UP("scale up"),
    SCALE_DOWN("scale down"),
    SCALE_AND_ROLLBACK("rollback"),
}

enum class ChurnMode(val label: String) {
    STEADY("steady members"),
    ADD_MEMBER("add member"),
    GRACEFUL_LEAVE("graceful leave"),
    EXPIRE_MEMBER("member expiry"),
    ROLLING_RESTART("rolling restart"),
    REPLACE_MEMBER("member replacement"),
}

enum class ConcurrencyMode(val label: String) {
    UNIFORM("uniform capacity"),
    FIRST_MEMBER_HAS_HIGHER_CAPACITY("weighted capacity"),
}

private class ScenarioClock(
    private var current: Instant,
    private val zone: ZoneId,
) : Clock() {
    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock =
        ScenarioClock(current, zone)

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
        current = current.plus(duration)
    }
}
