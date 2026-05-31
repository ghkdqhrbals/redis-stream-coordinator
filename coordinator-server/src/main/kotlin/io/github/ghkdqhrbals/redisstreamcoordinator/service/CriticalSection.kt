package io.github.ghkdqhrbals.redisstreamcoordinator.service

/**
 * Runs the annotated coordinator service method under a JVM-local monitor and the configured state mutex.
 *
 * The local monitor provides the same single-instance serialization intent as Kotlin's `@Synchronized`.
 * The state mutex extends the critical section across coordinator instances when Redis-backed locking is enabled.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
internal annotation class CriticalSection(
    val operation: String,
)
