package io.github.ghkdqhrbals.redisstreamcoordinator.config

import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.Clock
import java.time.Instant

enum class CoordinatorAuditAction {
    CREATE_STREAM,
    CREATE_GROUP,
    DELETE_GROUP,
    SCALE_STREAM,
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
    val reshardingId: String? = null,
    val route: String? = null,
    val queryString: String? = null,
    val requestId: String? = null,
    val requestedBy: String? = null,
    val reason: String? = null,
    val clientAddress: String? = null,
    val userAgent: String? = null,
    val roles: List<String> = emptyList(),
    val durationMs: Long? = null,
    val requestBodySha256: String? = null,
    val requestSummary: Map<String, String> = emptyMap(),
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
            "audit action={} outcome={} status={} principal={} requestedBy={} reason={} requestId={} coordinatorId={} streamPrefix={} consumerGroup={} reshardingId={} method={} path={} route={} queryString={} durationMs={} roles={} requestSummary={} requestBodySha256={}",
            event.action,
            event.outcome,
            event.status,
            event.principal ?: "anonymous",
            event.requestedBy ?: "",
            event.reason ?: "",
            event.requestId ?: "",
            event.coordinatorId,
            event.streamPrefix ?: "",
            event.consumerGroup ?: "",
            event.reshardingId ?: "",
            event.method,
            event.path,
            event.route ?: "",
            event.queryString ?: "",
            event.durationMs ?: "",
            event.roles.joinToString(","),
            event.requestSummary,
            event.requestBodySha256 ?: "",
        )
    }
}

@Component
@ConditionalOnProperty(prefix = "coordinator.audit", name = ["sink"], havingValue = "redis")
class RedisCoordinatorAuditLogSink(
    private val redisCommands: CoordinatorRedisCommands,
    private val objectMapper: ObjectMapper,
    private val properties: CoordinatorProperties,
) : CoordinatorAuditLogSink {
    constructor(
        redisTemplate: StringRedisTemplate,
        objectMapper: ObjectMapper,
        properties: CoordinatorProperties,
    ) : this(CoordinatorRedisCommands(redisTemplate = redisTemplate), objectMapper, properties)

    override fun append(event: CoordinatorAuditEvent) {
        val streamPrefix = event.streamPrefix ?: return
        val key = auditKey(streamPrefix, event.consumerGroup)
        val maxEntries = properties.audit.redisMaxEntries.coerceAtLeast(1)
        redisCommands.rightPushAndTrim(key, objectMapper.writeValueAsString(event), maxEntries.toLong())
    }

    private fun auditKey(streamPrefix: String, consumerGroup: String?): String {
        val tag = if (consumerGroup == null) "{$streamPrefix}" else "{$streamPrefix:$consumerGroup}"
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
        reshardingId: String? = null,
        route: String? = null,
        queryString: String? = null,
        requestId: String? = null,
        requestedBy: String? = null,
        reason: String? = null,
        clientAddress: String? = null,
        userAgent: String? = null,
        roles: List<String> = emptyList(),
        durationMs: Long? = null,
        requestBodySha256: String? = null,
        requestSummary: Map<String, String> = emptyMap(),
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
                reshardingId = reshardingId,
                route = route,
                queryString = queryString,
                requestId = requestId,
                requestedBy = requestedBy,
                reason = reason,
                clientAddress = clientAddress,
                userAgent = userAgent,
                roles = roles,
                durationMs = durationMs,
                requestBodySha256 = requestBodySha256,
                requestSummary = requestSummary,
            ),
        )
    }
}
