package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "coordinator.store.type=memory",
        "coordinator.streams.provisioning-enabled=true",
        "coordinator.api.rate-limit.enabled=true",
        "coordinator.api.rate-limit.admin-mutations-per-minute=120",
        "coordinator.redis-cluster.node-mappings[0].advertised-host=127.0.0.1",
        "coordinator.redis-cluster.node-mappings[0].advertised-port=7001",
        "coordinator.redis-cluster.node-mappings[0].connect-host=redis-node-1",
        "coordinator.redis-cluster.node-mappings[0].connect-port=7001",
        "coordinator.redis-cluster.node-mappings[1].advertised-host=127.0.0.1",
        "coordinator.redis-cluster.node-mappings[1].advertised-port=7002",
        "coordinator.redis-cluster.node-mappings[1].connect-host=redis-node-2",
        "coordinator.redis-cluster.node-mappings[1].connect-port=7002",
        "coordinator.redis-cluster.node-mappings[2].advertised-host=127.0.0.1",
        "coordinator.redis-cluster.node-mappings[2].advertised-port=7003",
        "coordinator.redis-cluster.node-mappings[2].connect-host=redis-node-3",
        "coordinator.redis-cluster.node-mappings[2].connect-port=7003",
    ],
)
class CoordinatorDockerConfigurationTest {
    @Autowired
    private lateinit var properties: CoordinatorProperties

    @Test
    fun `docker compose coordinator settings bind to coordinator properties`() {
        assertTrue(properties.streams.provisioningEnabled)
        assertTrue(properties.api.rateLimit.enabled)
        assertEquals(120, properties.api.rateLimit.adminMutationsPerMinute)

        assertEquals(
            listOf(
                CoordinatorProperties.NodeMapping("127.0.0.1", 7001, "redis-node-1", 7001),
                CoordinatorProperties.NodeMapping("127.0.0.1", 7002, "redis-node-2", 7002),
                CoordinatorProperties.NodeMapping("127.0.0.1", 7003, "redis-node-3", 7003),
            ),
            properties.redisCluster.nodeMappings,
        )
    }
}
