package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.mockito.Mockito
import org.mockito.stubbing.Answer
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import java.time.Duration

class CoordinatorProvisioningFailureIntegrationTest {
    @Test
    fun `mocked provisioning failure during create does not publish group metadata`() {
        val store = InMemoryCoordinatorStateStore()
        val calls = mutableListOf<RedisStreamShardProvisioningPlan>()
        val provisioner = mockedProvisioner(failingShardCounts = setOf(2), calls = calls)
        val service = service(store, provisioner)

        val error = assertFailsWith<IllegalStateException> {
            service.createGroup(
                "mock-create-failure",
                "orders-consumer",
                createGroupRequest(initialShardCount = 2),
            )
        }

        assertEquals("mock provisioning failure for shard count 2", error.message)
        assertTrue(store.list().isEmpty())
        assertEquals(listOf(2), calls.map { it.shardCount })
        assertEquals(1, Mockito.mockingDetails(provisioner).invocations.size)
    }

    @Test
    fun `mocked losing create race does not provision rejected shard plan`() {
        val store = LosingCreateRaceStore()
        val calls = mutableListOf<RedisStreamShardProvisioningPlan>()
        val provisioner = mockedProvisioner(failingShardCounts = emptySet(), calls = calls)
        val service = service(store, provisioner)

        val error = assertFailsWith<CoordinatorException> {
            service.createGroup(
                "mock-create-race",
                "orders-consumer",
                createGroupRequest(initialShardCount = 2),
            )
        }

        assertEquals(CoordinatorError.GROUP_ALREADY_EXISTS, error.error)
        assertTrue(calls.isEmpty())
    }

    @Test
    fun `mocked save conflict during scale does not provision until preparing state is committed`() {
        val store = CopyingContendedStateStore(conflictsBeforeSave = 1)
        val calls = mutableListOf<RedisStreamShardProvisioningPlan>()
        val provisioner = mockedProvisioner(failingShardCounts = emptySet(), calls = calls)
        val service = service(store, provisioner)
        service.createGroup("mock-scale-conflict", "orders-consumer", createGroupRequest(initialShardCount = 2))
        calls.clear()

        val migration = service.scaleGroup(
            "mock-scale-conflict",
            "orders-consumer",
            ScaleGroupRequest(
                targetShardCount = 4,
                requestedBy = "test",
                reason = "retry after prepare save conflict",
            ),
        )
        val after = service.getGroup("mock-scale-conflict", "orders-consumer")

        assertEquals(MigrationState.ACTIVE, migration.state)
        assertEquals(listOf(4), calls.map { it.shardCount })
    }

    @Test
    fun `mocked provisioning failure during scale keeps retryable preparing migration metadata`() {
        val store = InMemoryCoordinatorStateStore()
        val calls = mutableListOf<RedisStreamShardProvisioningPlan>()
        val provisioner = mockedProvisioner(failingShardCounts = setOf(4), calls = calls)
        val service = service(store, provisioner)
        service.createGroup("mock-scale-failure", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val before = service.getGroup("mock-scale-failure", "orders-consumer")

        val error = assertFailsWith<IllegalStateException> {
            service.scaleGroup(
                "mock-scale-failure",
                "orders-consumer",
                ScaleGroupRequest(
                    targetShardCount = 4,
                    requestedBy = "test",
                    reason = "mocked provisioning failure",
                ),
            )
        }
        val after = service.getGroup("mock-scale-failure", "orders-consumer")

        assertEquals("mock provisioning failure for shard count 4", error.message)
        val preparing = assertNotNull(after.activeMigration)
        assertEquals(MigrationState.PREPARING, preparing.state)
        assertEquals(before.metadataVersion, after.metadataVersion)
        assertEquals(listOf(2, 4), calls.map { it.shardCount })
        assertEquals(2, Mockito.mockingDetails(provisioner).invocations.size)

        val retryCalls = mutableListOf<RedisStreamShardProvisioningPlan>()
        val retryService = service(store, mockedProvisioner(failingShardCounts = emptySet(), calls = retryCalls))
        val migration = retryService.scaleGroup(
            "mock-scale-failure",
            "orders-consumer",
            ScaleGroupRequest(
                targetShardCount = 4,
                requestedBy = "test",
                reason = "retry prepared migration",
            ),
        )
        val activated = retryService.getGroup("mock-scale-failure", "orders-consumer")

        assertEquals(MigrationState.ACTIVE, migration.state)
        assertEquals(listOf(4), retryCalls.map { it.shardCount })
    }

    private fun mockedProvisioner(
        failingShardCounts: Set<Int>,
        calls: MutableList<RedisStreamShardProvisioningPlan>,
    ): StreamShardProvisioner =
        Mockito.mock(
            StreamShardProvisioner::class.java,
            Answer { invocation ->
                val plan = invocation.getArgument<RedisStreamShardProvisioningPlan>(0)
                calls += plan
                if (plan.shardCount in failingShardCounts) {
                    throw IllegalStateException("mock provisioning failure for shard count ${plan.shardCount}")
                }
                null
            },
        )

    private fun service(
        stateStore: CoordinatorStateStore,
        streamProvisioner: StreamShardProvisioner,
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
            stateStore = stateStore,
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = streamProvisioner,
        )

    private fun createGroupRequest(initialShardCount: Int): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )
}

private class LosingCreateRaceStore : CoordinatorStateStore {
    override fun contains(key: GroupKey): Boolean = false

    override fun get(key: GroupKey): GroupMetadata? = null

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean = false

    override fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean = false

    override fun save(key: GroupKey, group: GroupMetadata) = Unit

    override fun list(): List<GroupMetadata> = emptyList()
}

private class CopyingContendedStateStore(
    private var conflictsBeforeSave: Int,
) : CoordinatorStateStore {
    private val groups = linkedMapOf<GroupKey, GroupMetadata>()

    override fun contains(key: GroupKey): Boolean =
        key in groups

    override fun get(key: GroupKey): GroupMetadata? =
        groups[key]?.deepCopy()

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean {
        if (key in groups) {
            return false
        }
        val stored = group.deepCopy()
        stored.storeRevision = 1
        group.storeRevision = stored.storeRevision
        groups[key] = stored
        return true
    }

    override fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean {
        if (groups[key]?.storeRevision != expectedRevision) {
            return false
        }
        groups.remove(key)
        return true
    }

    override fun save(key: GroupKey, group: GroupMetadata) {
        if (conflictsBeforeSave > 0) {
            conflictsBeforeSave -= 1
            throw CoordinatorStateConflictException("injected save conflict")
        }
        val stored = group.deepCopy()
        stored.storeRevision = group.storeRevision + 1
        group.storeRevision = stored.storeRevision
        groups[key] = stored
    }

    override fun list(): List<GroupMetadata> =
        groups.values.map { it.deepCopy() }

    private fun GroupMetadata.deepCopy(): GroupMetadata =
        copy(
            consumerConcurrencyPolicy = consumerConcurrencyPolicy.copy(
                memberOverrides = consumerConcurrencyPolicy.memberOverrides.toMap(),
            ),
            members = members.mapValues { (_, member) -> member.copy() }.toMutableMap(),
            targetAssignments = targetAssignments
                .mapValues { (_, shards) -> shards.toMutableSet() }
                .toMutableMap(),
            migrations = migrations.mapValues { (_, migration) -> migration.copy() }.toMutableMap(),
        )
}
