package io.github.ghkdqhrbals.redisstreamcoordinator.config.audit

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorAuditAction
import jakarta.servlet.http.HttpServletRequest

internal data class AuditTarget(
    val action: CoordinatorAuditAction,
    val streamPrefix: String,
    val consumerGroup: String?,
    val reshardingId: String? = null,
)

internal fun HttpServletRequest.auditTarget(): AuditTarget? {
    val path = requestURI.removePrefix("/")
    val parts = path.split("/")
    if (parts.size < 4 || parts[0] != "coord" || parts[1] != "v1" || parts[2] != "streams") {
        return null
    }

    if (method.equals("POST", ignoreCase = true) && parts.size == 4) {
        return AuditTarget(CoordinatorAuditAction.CREATE_STREAM, parts[3], consumerGroup = null)
    }

    if (method.equals("POST", ignoreCase = true) && parts.size == 5 && parts[4] == "scale") {
        return AuditTarget(CoordinatorAuditAction.SCALE_STREAM, parts[3], consumerGroup = null)
    }

    if (parts.size < 6 || parts[4] != "groups") {
        return null
    }

    val streamPrefix = parts[3]
    val consumerGroup = parts[5]
    val tail = parts.drop(6)
    return when {
        method.equals("POST", ignoreCase = true) && tail.isEmpty() ->
            AuditTarget(CoordinatorAuditAction.CREATE_GROUP, streamPrefix, consumerGroup)
        method.equals("DELETE", ignoreCase = true) && tail.isEmpty() ->
            AuditTarget(CoordinatorAuditAction.DELETE_GROUP, streamPrefix, consumerGroup)
        method.equals("POST", ignoreCase = true) && tail == listOf("scale") ->
            AuditTarget(CoordinatorAuditAction.SCALE_GROUP, streamPrefix, consumerGroup)
        method.equals("POST", ignoreCase = true) && tail.size == 3 && tail[0] == "migrations" && tail[2] == "rollback" ->
            AuditTarget(CoordinatorAuditAction.ROLLBACK_MIGRATION, streamPrefix, consumerGroup, reshardingId = tail[1])
        else -> null
    }
}
