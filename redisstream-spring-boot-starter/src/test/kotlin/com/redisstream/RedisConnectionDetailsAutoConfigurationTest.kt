package com.redisstream

import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RedisConnectionDetailsAutoConfigurationTest {
    private val contextRunner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                RedisConnectionDetailsAutoConfiguration::class.java,
                DataRedisAutoConfiguration::class.java,
            ),
        )

    @Test
    fun `blank cluster nodes are ignored so standalone redis remains usable`() {
        contextRunner
            .withPropertyValues(
                "spring.data.redis.cluster.nodes=",
                "spring.data.redis.host=127.0.0.1",
                "spring.data.redis.port=6380",
                "spring.data.redis.database=2",
            )
            .run { context ->
                val factory = context.getBean(LettuceConnectionFactory::class.java)

                assertFalse(factory.isClusterAware)
                assertFalse(factory.isRedisSentinelAware)
                assertNull(factory.clusterConfiguration)
                assertEquals("127.0.0.1", factory.standaloneConfiguration.hostName)
                assertEquals(6380, factory.standaloneConfiguration.port)
                assertEquals(2, factory.database)
            }
    }

    @Test
    fun `rediss url configures standalone redis with ssl`() {
        contextRunner
            .withPropertyValues(
                "spring.data.redis.cluster.nodes=",
                "spring.data.redis.url=rediss://default:secret@redis.example.com:6380/3",
            )
            .run { context ->
                val factory = context.getBean(LettuceConnectionFactory::class.java)

                assertFalse(factory.isClusterAware)
                assertFalse(factory.isRedisSentinelAware)
                assertEquals("redis.example.com", factory.standaloneConfiguration.hostName)
                assertEquals(6380, factory.standaloneConfiguration.port)
                assertEquals(3, factory.database)
                assertTrue(factory.isUseSsl)
            }
    }

    @Test
    fun `cluster nodes configure cluster redis`() {
        contextRunner
            .withPropertyValues(
                "spring.data.redis.cluster.nodes=127.0.0.1:7001,127.0.0.1:7002",
            )
            .run { context ->
                val factory = context.getBean(LettuceConnectionFactory::class.java)
                val clusterConfiguration = assertNotNull(factory.clusterConfiguration)

                assertTrue(factory.isClusterAware)
                assertEquals(
                    setOf("127.0.0.1:7001", "127.0.0.1:7002"),
                    clusterConfiguration.clusterNodes.map { "${it.host}:${it.port}" }.toSet(),
                )
            }
    }

    @Test
    fun `sentinel properties configure sentinel redis`() {
        contextRunner
            .withPropertyValues(
                "spring.data.redis.sentinel.master=rsc-master",
                "spring.data.redis.sentinel.nodes=127.0.0.1:26379,127.0.0.1:26380",
            )
            .run { context ->
                val factory = context.getBean(LettuceConnectionFactory::class.java)
                val sentinelConfiguration = assertNotNull(factory.sentinelConfiguration)

                assertTrue(factory.isRedisSentinelAware)
                assertEquals("rsc-master", sentinelConfiguration.master?.name)
                assertEquals(
                    setOf("127.0.0.1:26379", "127.0.0.1:26380"),
                    sentinelConfiguration.sentinels.map { "${it.host}:${it.port}" }.toSet(),
                )
            }
    }
}
