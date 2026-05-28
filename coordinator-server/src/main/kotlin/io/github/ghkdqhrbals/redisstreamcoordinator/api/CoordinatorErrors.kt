package io.github.ghkdqhrbals.redisstreamcoordinator.api

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

enum class CoordinatorError(
    val status: HttpStatus,
    val code: String,
    val defaultMessage: String,
) {
    GROUP_ALREADY_EXISTS(HttpStatus.CONFLICT, "GROUP_ALREADY_EXISTS", "Group already exists"),
    GROUP_NOT_FOUND(HttpStatus.NOT_FOUND, "GROUP_NOT_FOUND", "Group not found"),
    ACTIVE_MIGRATION_EXISTS(HttpStatus.CONFLICT, "ACTIVE_MIGRATION_EXISTS", "Group already has an active migration"),
    MIGRATION_NOT_FOUND(HttpStatus.NOT_FOUND, "MIGRATION_NOT_FOUND", "Migration not found"),
    ROLLBACK_NOT_ALLOWED(HttpStatus.UNPROCESSABLE_ENTITY, "ROLLBACK_NOT_ALLOWED", "Migration cannot be rolled back"),
    STATE_VERSION_CONFLICT(
        HttpStatus.CONFLICT,
        "STATE_VERSION_CONFLICT",
        "Coordinator state changed concurrently; retry the request",
    ),
    REDIS_NOT_CONFIGURED(
        HttpStatus.SERVICE_UNAVAILABLE,
        "REDIS_NOT_CONFIGURED",
        "Redis Stream provisioning is enabled but Redis is not configured",
    ),
    REDIS_STREAM_PROVISIONING_FAILED(
        HttpStatus.SERVICE_UNAVAILABLE,
        "REDIS_STREAM_PROVISIONING_FAILED",
        "Failed to provision Redis Stream consumer group",
    ),
    COORDINATOR_STATE_MUTEX_UNAVAILABLE(
        HttpStatus.SERVICE_UNAVAILABLE,
        "COORDINATOR_STATE_MUTEX_UNAVAILABLE",
        "Coordinator state mutex is unavailable",
    ),
    RATE_LIMIT_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, "RATE_LIMIT_EXCEEDED", "Coordinator API rate limit exceeded"),
    INVALID_REQUEST(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Invalid request"),
}

class CoordinatorException(
    val error: CoordinatorError,
    override val message: String = error.defaultMessage,
    cause: Throwable? = null,
) : RuntimeException(message, cause) {
    val status: HttpStatus = error.status
    val errorCode: String = error.code
}

data class ErrorResponse(
    val status: String,
    val errorCode: String,
    val message: String,
)

@RestControllerAdvice
class CoordinatorExceptionHandler {
    @ExceptionHandler(CoordinatorException::class)
    fun coordinatorException(error: CoordinatorException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(error.status).body(
            ErrorResponse(error.status.name, error.errorCode, error.message),
        )

    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun validationException(error: MethodArgumentNotValidException): ResponseEntity<ErrorResponse> =
        ResponseEntity.status(CoordinatorError.INVALID_REQUEST.status).body(
            ErrorResponse(
                CoordinatorError.INVALID_REQUEST.status.name,
                CoordinatorError.INVALID_REQUEST.code,
                error.bindingResult.fieldErrors.joinToString("; ") { "${it.field}: ${it.defaultMessage}" },
            ),
        )
}
