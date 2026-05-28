package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorException
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorMutexStore
import io.github.ghkdqhrbals.redisstreamcoordinator.service.RedisCoordinatorStateMutex
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import java.time.Duration

class CoordinatorStateMutexTest {
    @Test
    fun `redis state mutex allows only one critical section at a time`() {
        val store = InMemoryMutexStore()
        val first = guard("coordinator-a", store)
        val second = guard("coordinator-b", store)
        var secondRan = false

        val nestedResult = first.withCriticalSection("heartbeat") {
            second.tryCriticalSection("heartbeat") {
                secondRan = true
                "unexpected"
            }
        }

        assertNull(nestedResult)
        assertFalse(secondRan)
        assertEquals("after-release", second.tryCriticalSection("heartbeat") { "after-release" })

        first.close()
        second.close()
    }

    @Test
    fun `redis state mutex returns service unavailable when mutex cannot be acquired`() {
        val store = InMemoryMutexStore()
        store.acquire("redis-stream:coord:test:state-mutex", "external-owner", Duration.ofSeconds(30))
        val guard = guard("coordinator-a", store)

        val error = assertFailsWith<CoordinatorException> {
            guard.withCriticalSection("scale-group") {
                "unexpected"
            }
        }

        assertEquals(CoordinatorError.COORDINATOR_STATE_MUTEX_UNAVAILABLE, error.error)
        guard.close()
    }

    private fun guard(ownerId: String, store: CoordinatorMutexStore): RedisCoordinatorStateMutex =
        RedisCoordinatorStateMutex(
            ownerId = ownerId,
            mutexKey = "redis-stream:coord:test:state-mutex",
            mutexTtl = Duration.ofSeconds(30),
            acquireTimeout = Duration.ZERO,
            retryInterval = Duration.ofMillis(1),
            mutexStore = store,
        )

    private class InMemoryMutexStore : CoordinatorMutexStore {
        private var token: String? = null

        @Synchronized
        override fun acquire(key: String, token: String, ttl: Duration): Boolean {
            if (this.token != null) {
                return false
            }
            this.token = token
            return true
        }

        @Synchronized
        override fun renew(key: String, token: String, ttl: Duration): Boolean =
            this.token == token

        @Synchronized
        override fun release(key: String, token: String): Boolean =
            if (this.token == token) {
                this.token = null
                true
            } else {
                false
            }
    }
}
