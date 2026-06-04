package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorException
import org.slf4j.LoggerFactory
import org.springframework.context.event.ContextClosedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Prevents a terminating coordinator process from accepting new metadata mutations.
 *
 * Rolling updates can briefly route traffic to both the old and new coordinator pods. The Redis
 * mutex serializes those pods globally; this local gate additionally makes the old pod fail new
 * critical sections after shutdown begins while allowing already-entered short sections to finish.
 */
@Component
class CoordinatorShutdownGate {
    private val logger = LoggerFactory.getLogger(CoordinatorShutdownGate::class.java)
    private val terminating = AtomicBoolean(false)
    private val inFlightCriticalSections = AtomicInteger(0)

    val isTerminating: Boolean
        get() = terminating.get()

    val activeCriticalSections: Int
        get() = inFlightCriticalSections.get()

    fun enter(operation: String): CriticalSectionLease {
        rejectIfTerminating(operation)
        inFlightCriticalSections.incrementAndGet()
        if (terminating.get()) {
            inFlightCriticalSections.decrementAndGet()
            rejectIfTerminating(operation)
        }
        return CriticalSectionLease()
    }

    fun markTerminating() {
        if (terminating.compareAndSet(false, true)) {
            logger.info(
                "Coordinator is entering terminating mode; active critical sections={}",
                inFlightCriticalSections.get(),
            )
        }
    }

    @EventListener(ContextClosedEvent::class)
    fun onContextClosed() {
        markTerminating()
    }

    private fun rejectIfTerminating(operation: String) {
        if (terminating.get()) {
            throw CoordinatorException(
                CoordinatorError.COORDINATOR_TERMINATING,
                "Coordinator is terminating; retry $operation against another coordinator instance",
            )
        }
    }

    inner class CriticalSectionLease : AutoCloseable {
        private val closed = AtomicBoolean(false)

        override fun close() {
            if (closed.compareAndSet(false, true)) {
                inFlightCriticalSections.decrementAndGet()
            }
        }
    }
}
