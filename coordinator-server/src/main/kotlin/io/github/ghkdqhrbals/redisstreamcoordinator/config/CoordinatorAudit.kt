package io.github.ghkdqhrbals.redisstreamcoordinator.config

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.Clock
import java.time.Instant

enum class CoordinatorAuditAction {
    CREATE_GROUP,
    SCALE_GROUP,
    UPDATE_CONSUMER_CONCURRENCY,
    ROLLBACK_MIGRATION,
}

data class CoordinatorAuditEvent(
    val timestamp: Instant,
    val coordinatorId: String,
    val action: CoordinatorAuditAction,
    val outcome: String,
    val status: Int,
    val principal: String?,
    val method: String,
    val path: String,
    val streamPrefix: String?,
    val consumerGroup: String?,
    val migrationId: String? = null,
)

interface CoordinatorAuditLogSink {
    fun append(event: CoordinatorAuditEvent)
}

@Component
@ConditionalOnProperty(prefix = "coordinator.audit", name = ["sink"], havingValue = "log", matchIfMissing = true)
class StructuredCoordinatorAuditLogSink : CoordinatorAuditLogSink {
    private val logger = LoggerFactory.getLogger("redis-stream-coordinator.audit")

    override fun append(event: CoordinatorAuditEvent) {
        logger.info(
            "audit action={} outcome={} status={} principal={} coordinatorId={} streamPrefix={} consumerGroup={} migrationId={} method={} path={}",
            event.action,
            event.outcome,
            event.status,
            event.principal ?: "anonymous",
            event.coordinatorId,
            event.streamPrefix ?: "",
            event.consumerGroup ?: "",
            event.migrationId ?: "",
            event.method,
            event.path,
        )
    }
}

@Component
@ConditionalOnProperty(prefix = "coordinator.audit", name = ["sink"], havingValue = "redis")
class RedisCoordinatorAuditLogSink(
    private val redisTemplate: StringRedisTemplate,
    private val objectMapper: ObjectMapper,
    private val properties: CoordinatorProperties,
) : CoordinatorAuditLogSink {
    override fun append(event: CoordinatorAuditEvent) {
        val streamPrefix = event.streamPrefix ?: return
        val consumerGroup = event.consumerGroup ?: return
        val key = auditKey(streamPrefix, consumerGroup)
        val maxEntries = properties.audit.redisMaxEntries.coerceAtLeast(1)
        redisTemplate.opsForList().rightPush(key, objectMapper.writeValueAsString(event))
        redisTemplate.opsForList().trim(key, -maxEntries, -1L)
    }

    private fun auditKey(streamPrefix: String, consumerGroup: String): String {
        val tag = "{$streamPrefix:$consumerGroup}"
        return "${properties.store.keyPrefix}:$tag:admin:audit"
    }
}

class CoordinatorAuditLogger(
    private val properties: CoordinatorProperties,
    private val sink: CoordinatorAuditLogSink,
    private val clock: Clock = Clock.systemUTC(),
) {
    fun append(
        action: CoordinatorAuditAction,
        outcome: String,
        status: Int,
        principal: String?,
        method: String,
        path: String,
        streamPrefix: String?,
        consumerGroup: String?,
        migrationId: String? = null,
    ) {
        sink.append(
            CoordinatorAuditEvent(
                timestamp = Instant.now(clock),
                coordinatorId = properties.id,
                action = action,
                outcome = outcome,
                status = status,
                principal = principal,
                method = method,
                path = path,
                streamPrefix = streamPrefix,
                consumerGroup = consumerGroup,
                migrationId = migrationId,
            ),
        )
    }
}
