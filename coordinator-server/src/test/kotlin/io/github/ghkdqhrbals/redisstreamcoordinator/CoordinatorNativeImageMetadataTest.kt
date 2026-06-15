package io.github.ghkdqhrbals.redisstreamcoordinator

import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertNotNull

class CoordinatorNativeImageMetadataTest {
    @Test
    fun `native image metadata registers Spring Data Redis cluster connection proxy`() {
        val resource = javaClass.classLoader.getResource(
            "META-INF/native-image/io.github.ghkdqhrbals/redis-stream-coordinator/proxy-config.json",
        )

        assertNotNull(resource)
        val metadata = resource.readText()
        assertContains(metadata, "org.springframework.data.redis.connection.RedisClusterConnection")
        assertContains(metadata, "org.springframework.data.redis.connection.DefaultedRedisClusterConnection")
        assertContains(metadata, "org.springframework.data.redis.connection.RedisConnection")
    }
}
