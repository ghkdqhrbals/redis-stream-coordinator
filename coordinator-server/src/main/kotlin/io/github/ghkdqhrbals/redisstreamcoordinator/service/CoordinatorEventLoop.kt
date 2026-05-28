package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.stereotype.Component
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Component
class CoordinatorEventLoop(
    private val properties: CoordinatorProperties,
    private val coordinator: CoordinatorService,
) : SmartLifecycle {
    private val logger = LoggerFactory.getLogger(CoordinatorEventLoop::class.java)
    private val running = AtomicBoolean(false)
    private var executor: ScheduledExecutorService? = null
    private var task: ScheduledFuture<*>? = null

    override fun start() {
        if (!properties.loop.enabled || !running.compareAndSet(false, true)) {
            return
        }
        val intervalMs = properties.loop.tickInterval.toMillis().coerceAtLeast(1)
        executor = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "redis-stream-coordinator-loop-${properties.id}").apply {
                isDaemon = true
            }
        }
        task = executor!!.scheduleWithFixedDelay(
            { runCatching { coordinator.tick() }.onFailure { logger.warn("Coordinator event loop tick failed", it) } },
            intervalMs,
            intervalMs,
            TimeUnit.MILLISECONDS,
        )
    }

    override fun stop() {
        if (!running.compareAndSet(true, false)) {
            return
        }
        task?.cancel(false)
        executor?.shutdownNow()
        task = null
        executor = null
    }

    override fun isRunning(): Boolean =
        running.get()

    override fun isAutoStartup(): Boolean =
        properties.loop.enabled
}
