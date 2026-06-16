package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorExceptionHandler
import io.github.ghkdqhrbals.redisstreamcoordinator.store.CoordinatorStateSchemaException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CoordinatorExceptionHandlerTest {
    @Test
    fun `corrupt coordinator metadata is reported as coordinator error response`() {
        val response = CoordinatorExceptionHandler()
            .coordinatorStateSchemaException(CoordinatorStateSchemaException("Invalid stream metadata"))

        assertEquals(CoordinatorError.COORDINATOR_METADATA_CORRUPT.status, response.statusCode)
        assertEquals(CoordinatorError.COORDINATOR_METADATA_CORRUPT.code, response.body?.errorCode)
        assertEquals("Invalid stream metadata", response.body?.message)
    }
}
