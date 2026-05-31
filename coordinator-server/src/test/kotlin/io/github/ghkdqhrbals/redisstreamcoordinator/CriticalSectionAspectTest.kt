package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.service.CriticalSection
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CriticalSectionAspect
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorStateMutex
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.reflect.MethodSignature
import org.mockito.Mockito
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CriticalSectionAspectTest {
    @Test
    fun `critical section annotation applies local monitor and coordinator mutex`() {
        val mutex = RecordingStateMutex()
        val aspect = CriticalSectionAspect(mutex)
        val target = AnnotatedTarget()
        val method = AnnotatedTarget::class.java.getMethod("guarded")
        val signature = Mockito.mock(MethodSignature::class.java)
        val joinPoint = Mockito.mock(ProceedingJoinPoint::class.java)

        Mockito.`when`(signature.method).thenReturn(method)
        Mockito.`when`(joinPoint.signature).thenReturn(signature)
        Mockito.`when`(joinPoint.target).thenReturn(target)
        Mockito.`when`(joinPoint.proceed()).thenAnswer {
            assertTrue(Thread.holdsLock(target))
            "ok"
        }

        val result = aspect.around(joinPoint)

        assertEquals("ok", result)
        assertEquals(listOf("test-operation"), mutex.operations)
    }

    private class AnnotatedTarget {
        @CriticalSection(operation = "test-operation")
        fun guarded() {
        }
    }

    private class RecordingStateMutex : CoordinatorStateMutex {
        val operations = mutableListOf<String>()

        override fun <T> withCriticalSection(operation: String, block: () -> T): T {
            operations += operation
            return block()
        }

        override fun <T> tryCriticalSection(operation: String, block: () -> T): T? {
            operations += operation
            return block()
        }
    }
}
