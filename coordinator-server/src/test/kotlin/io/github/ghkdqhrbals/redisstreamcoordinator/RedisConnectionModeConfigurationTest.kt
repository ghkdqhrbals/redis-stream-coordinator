package io.github.ghkdqhrbals.redisstreamcoordinator

import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RedisConnectionModeConfigurationTest {
    @Nested
    @SpringBootTest(
        properties = [
            "coordinator.store.type=memory",
            "spring.data.redis.cluster.nodes=",
            "spring.data.redis.host=127.0.0.1",
            "spring.data.redis.port=6380",
            "spring.data.redis.database=2",
        ],
    )
    inner class StandaloneMode {
        @Autowired
        private lateinit var redisConnectionFactory: LettuceConnectionFactory

        @Test
        fun `blank cluster nodes fall back to standalone redis`() {
            assertFalse(redisConnectionFactory.isClusterAware)
            assertFalse(redisConnectionFactory.isRedisSentinelAware)
            assertNull(redisConnectionFactory.clusterConfiguration)
            assertNull(redisConnectionFactory.sentinelConfiguration)
            assertEquals("127.0.0.1", redisConnectionFactory.standaloneConfiguration.hostName)
            assertEquals(6380, redisConnectionFactory.standaloneConfiguration.port)
            assertEquals(2, redisConnectionFactory.database)
        }
    }

    @Nested
    @SpringBootTest(
        properties = [
            "coordinator.store.type=memory",
            "spring.data.redis.cluster.nodes=",
            "spring.data.redis.url=rediss://default:secret@redis.example.com:6380/3",
        ],
    )
    inner class StandaloneUrlMode {
        @Autowired
        private lateinit var redisConnectionFactory: LettuceConnectionFactory

        @Test
        fun `rediss url configures standalone redis with ssl`() {
            assertFalse(redisConnectionFactory.isClusterAware)
            assertFalse(redisConnectionFactory.isRedisSentinelAware)
            assertEquals("redis.example.com", redisConnectionFactory.standaloneConfiguration.hostName)
            assertEquals(6380, redisConnectionFactory.standaloneConfiguration.port)
            assertEquals(3, redisConnectionFactory.database)
            assertTrue(redisConnectionFactory.isUseSsl)
        }
    }

    @Nested
    @SpringBootTest(
        properties = [
            "coordinator.store.type=memory",
            "spring.data.redis.cluster.nodes=127.0.0.1:7001,127.0.0.1:7002",
        ],
    )
    inner class ClusterMode {
        @Autowired
        private lateinit var redisConnectionFactory: LettuceConnectionFactory

        @Test
        fun `cluster nodes configure redis cluster connection factory`() {
            val clusterConfiguration = assertNotNull(redisConnectionFactory.clusterConfiguration)

            assertTrue(redisConnectionFactory.isClusterAware)
            assertFalse(redisConnectionFactory.isRedisSentinelAware)
            assertEquals(
                setOf("127.0.0.1:7001", "127.0.0.1:7002"),
                clusterConfiguration.clusterNodes.map { "${it.host}:${it.port}" }.toSet(),
            )
        }
    }

    @Nested
    @SpringBootTest(
        properties = [
            "coordinator.store.type=memory",
            "spring.data.redis.sentinel.master=rsc-master",
            "spring.data.redis.sentinel.nodes=127.0.0.1:26379,127.0.0.1:26380",
        ],
    )
    inner class SentinelMode {
        @Autowired
        private lateinit var redisConnectionFactory: LettuceConnectionFactory

        @Test
        fun `sentinel properties configure redis sentinel connection factory`() {
            val sentinelConfiguration = assertNotNull(redisConnectionFactory.sentinelConfiguration)

            assertFalse(redisConnectionFactory.isClusterAware)
            assertTrue(redisConnectionFactory.isRedisSentinelAware)
            assertEquals("rsc-master", sentinelConfiguration.master?.name)
            assertEquals(
                setOf("127.0.0.1:26379", "127.0.0.1:26380"),
                sentinelConfiguration.sentinels.map { "${it.host}:${it.port}" }.toSet(),
            )
        }
    }
}
