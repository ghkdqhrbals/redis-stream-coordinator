package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorException
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

interface CoordinatorStateMutex : AutoCloseable {
    fun <T> withCriticalSection(operation: String, block: () -> T): T

    fun <T> tryCriticalSection(operation: String, block: () -> T): T?

    override fun close() {
    }
}

object LocalCoordinatorStateMutex : CoordinatorStateMutex {
    override fun <T> withCriticalSection(operation: String, block: () -> T): T =
        block()

    override fun <T> tryCriticalSection(operation: String, block: () -> T): T? =
        block()
}

interface CoordinatorMutexStore {
    fun acquire(key: String, token: String, ttl: Duration): Boolean
    fun renew(key: String, token: String, ttl: Duration): Boolean
    fun release(key: String, token: String): Boolean
}

class RedisCoordinatorMutexStore(
    private val redisTemplate: StringRedisTemplate,
) : CoordinatorMutexStore {
    override fun acquire(key: String, token: String, ttl: Duration): Boolean =
        redisTemplate.opsForValue().setIfAbsent(key, token, ttl) == true

    override fun renew(key: String, token: String, ttl: Duration): Boolean =
        redisTemplate.execute(RENEW_SCRIPT, listOf(key), token, ttl.toMillis().coerceAtLeast(1).toString()) == 1L

    override fun release(key: String, token: String): Boolean =
        redisTemplate.execute(RELEASE_SCRIPT, listOf(key), token) == 1L

    companion object {
        private val RENEW_SCRIPT = DefaultRedisScript(
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              redis.call('PEXPIRE', KEYS[1], ARGV[2])
              return 1
            end
            return 0
            """.trimIndent(),
            Long::class.java,
        )

        private val RELEASE_SCRIPT = DefaultRedisScript(
            """
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              redis.call('DEL', KEYS[1])
              return 1
            end
            return 0
            """.trimIndent(),
            Long::class.java,
        )
    }
}

class RedisCoordinatorStateMutex(
    private val ownerId: String,
    private val mutexKey: String,
    private val mutexTtl: Duration,
    private val acquireTimeout: Duration,
    private val retryInterval: Duration,
    private val mutexStore: CoordinatorMutexStore,
) : CoordinatorStateMutex {
    private val logger = LoggerFactory.getLogger(RedisCoordinatorStateMutex::class.java)
    private val renewalExecutor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "redis-stream-coordinator-state-mutex-$ownerId").apply {
            isDaemon = true
        }
    }

    override fun <T> withCriticalSection(operation: String, block: () -> T): T {
        val lease = acquire(operation, wait = true)
            ?: throw CoordinatorException(
                CoordinatorError.COORDINATOR_STATE_MUTEX_UNAVAILABLE,
                "Coordinator state mutex is currently held by another instance; retry the request",
            )
        return lease.use(block)
    }

    override fun <T> tryCriticalSection(operation: String, block: () -> T): T? {
        val lease = acquire(operation, wait = false) ?: return null
        return lease.use(block)
    }

    private fun acquire(operation: String, wait: Boolean): HeldMutex? {
        val token = "$ownerId:${UUID.randomUUID()}"
        val deadline = System.nanoTime() + acquireTimeout.toNanos().coerceAtLeast(0)
        do {
            if (mutexStore.acquire(mutexKey, token, mutexTtl)) {
                return HeldMutex(operation, token, scheduleRenewal(operation, token))
            }
            if (!wait || acquireTimeout.isZero || acquireTimeout.isNegative) {
                return null
            }
            try {
                Thread.sleep(retryInterval.toMillis().coerceAtLeast(1))
            } catch (error: InterruptedException) {
                Thread.currentThread().interrupt()
                return null
            }
        } while (System.nanoTime() < deadline)
        return null
    }

    private fun scheduleRenewal(operation: String, token: String): ScheduledFuture<*> {
        val renewalIntervalMs = (mutexTtl.toMillis() / 3).coerceAtLeast(1)
        return renewalExecutor.scheduleWithFixedDelay(
            {
                runCatching {
                    if (!mutexStore.renew(mutexKey, token, mutexTtl)) {
                        logger.warn("Coordinator state mutex renewal failed for operation {}", operation)
                    }
                }.onFailure {
                    logger.warn("Coordinator state mutex renewal errored for operation {}", operation, it)
                }
            },
            renewalIntervalMs,
            renewalIntervalMs,
            TimeUnit.MILLISECONDS,
        )
    }

    override fun close() {
        renewalExecutor.shutdownNow()
    }

    private inner class HeldMutex(
        private val operation: String,
        private val token: String,
        private val renewal: ScheduledFuture<*>,
    ) {
        private val released = AtomicBoolean(false)

        fun <T> use(block: () -> T): T {
            try {
                return block()
            } finally {
                release()
            }
        }

        private fun release() {
            if (!released.compareAndSet(false, true)) {
                return
            }
            renewal.cancel(false)
            runCatching { mutexStore.release(mutexKey, token) }
                .onFailure { logger.warn("Coordinator state mutex release failed for operation {}", operation, it) }
        }
    }
}
