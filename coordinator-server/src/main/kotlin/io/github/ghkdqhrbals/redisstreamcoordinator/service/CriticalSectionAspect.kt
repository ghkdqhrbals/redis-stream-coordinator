package io.github.ghkdqhrbals.redisstreamcoordinator.service

import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.reflect.MethodSignature
import org.springframework.stereotype.Component

/**
 * Applies local synchronization and distributed coordinator locking for annotated service methods.
 */
@Aspect
@Component
internal class CriticalSectionAspect(
    private val stateMutex: CoordinatorStateMutex,
) {
    @Around("@annotation(io.github.ghkdqhrbals.redisstreamcoordinator.service.CriticalSection)")
    fun around(joinPoint: ProceedingJoinPoint): Any? {
        val criticalSection = (joinPoint.signature as MethodSignature)
            .method
            .getAnnotation(CriticalSection::class.java)
        val monitor = joinPoint.target ?: this
        return synchronized(monitor) {
            stateMutex.withCriticalSection(criticalSection.operation) {
                joinPoint.proceed()
            }
        }
    }
}
