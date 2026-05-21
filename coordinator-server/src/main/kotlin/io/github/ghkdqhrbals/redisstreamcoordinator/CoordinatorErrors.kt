package io.github.ghkdqhrbals.redisstreamcoordinator

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

class CoordinatorException(
    val status: HttpStatus,
    val errorCode: String,
    override val message: String,
) : RuntimeException(message)

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
        ResponseEntity.badRequest().body(
            ErrorResponse(
                HttpStatus.BAD_REQUEST.name,
                "INVALID_REQUEST",
                error.bindingResult.fieldErrors.joinToString("; ") { "${it.field}: ${it.defaultMessage}" },
            ),
        )
}
