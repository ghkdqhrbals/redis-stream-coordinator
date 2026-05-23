package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditAction
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditEvent
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.config.RedisCoordinatorAuditLogSink
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.springframework.data.redis.core.ListOperations
import org.springframework.data.redis.core.StringRedisTemplate
import tools.jackson.databind.ObjectMapper
import java.time.Instant

class CoordinatorAuditLogSinkTest {
    @Test
    fun `redis audit sink appends group-scoped event and trims retained entries`() {
        val redisTemplate = Mockito.mock(StringRedisTemplate::class.java)
        @Suppress("UNCHECKED_CAST")
        val listOperations = Mockito.mock(ListOperations::class.java) as ListOperations<String, String>
        val objectMapper = Mockito.mock(ObjectMapper::class.java)
        val event = CoordinatorAuditEvent(
            timestamp = Instant.parse("2026-05-23T00:00:00Z"),
            coordinatorId = "coordinator-a",
            action = CoordinatorAuditAction.SCALE_GROUP,
            outcome = "SUCCESS",
            status = 202,
            principal = "admin",
            method = "POST",
            path = "/coord/v1/streams/orders/groups/orders-consumer/scale",
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
        )
        val properties = CoordinatorProperties(
            store = CoordinatorProperties.Store(keyPrefix = "redis-stream:coord:test"),
            audit = CoordinatorProperties.Audit(redisMaxEntries = 25),
        )

        Mockito.`when`(redisTemplate.opsForList()).thenReturn(listOperations)
        Mockito.`when`(objectMapper.writeValueAsString(event)).thenReturn("""{"action":"SCALE_GROUP"}""")

        RedisCoordinatorAuditLogSink(redisTemplate, objectMapper, properties).append(event)

        val key = "redis-stream:coord:test:{orders:orders-consumer}:admin:audit"
        Mockito.verify(listOperations).rightPush(key, """{"action":"SCALE_GROUP"}""")
        Mockito.verify(listOperations).trim(key, -25L, -1L)
    }
}
