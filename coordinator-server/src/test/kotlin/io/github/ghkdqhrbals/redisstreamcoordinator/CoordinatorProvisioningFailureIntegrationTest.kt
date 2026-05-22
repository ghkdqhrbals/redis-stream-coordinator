package io.github.ghkdqhrbals.redisstreamcoordinator

import org.mockito.Mockito
import org.mockito.stubbing.Answer
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import java.time.Duration

class CoordinatorProvisioningFailureIntegrationTest {
    @Test
    fun `mocked provisioning failure during create does not publish group metadata`() {
        val store = InMemoryCoordinatorStateStore()
        val calls = mutableListOf<RedisStreamShardProvisioningPlan>()
        val provisioner = mockedProvisioner(failingVersions = setOf(1), calls = calls)
        val service = service(store, provisioner)

        val error = assertFailsWith<IllegalStateException> {
            service.createGroup(
                "mock-create-failure",
                "orders-consumer",
                createGroupRequest(initialShardCount = 2),
            )
        }

        assertEquals("mock provisioning failure for version 1", error.message)
        assertTrue(store.list().isEmpty())
        assertEquals(listOf(1), calls.map { it.streamVersion })
        assertEquals(1, Mockito.mockingDetails(provisioner).invocations.size)
    }

    @Test
    fun `mocked provisioning failure during scale keeps previous coordinator metadata`() {
        val store = InMemoryCoordinatorStateStore()
        val calls = mutableListOf<RedisStreamShardProvisioningPlan>()
        val provisioner = mockedProvisioner(failingVersions = setOf(2), calls = calls)
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

        assertEquals("mock provisioning failure for version 2", error.message)
        assertEquals(1, after.activeWriteVersion)
        assertEquals(setOf(1), after.readableVersions)
        assertEquals(null, after.activeMigration)
        assertEquals(before.metadataVersion, after.metadataVersion)
        assertEquals(listOf(1, 2), calls.map { it.streamVersion })
        assertEquals(2, Mockito.mockingDetails(provisioner).invocations.size)
    }

    private fun mockedProvisioner(
        failingVersions: Set<Int>,
        calls: MutableList<RedisStreamShardProvisioningPlan>,
    ): StreamShardProvisioner =
        Mockito.mock(
            StreamShardProvisioner::class.java,
            Answer { invocation ->
                val plan = invocation.getArgument<RedisStreamShardProvisioningPlan>(0)
                calls += plan
                if (plan.streamVersion in failingVersions) {
                    throw IllegalStateException("mock provisioning failure for version ${plan.streamVersion}")
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
            hashAlgorithm = "murmur3",
            requestedBy = "test",
        )
}
