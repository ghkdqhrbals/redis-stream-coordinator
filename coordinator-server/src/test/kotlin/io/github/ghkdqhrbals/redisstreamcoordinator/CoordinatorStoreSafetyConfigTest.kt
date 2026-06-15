package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorStoreSafetyConfig
import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CoordinatorStoreSafetyConfigTest {
    private val contextRunner = ApplicationContextRunner()
        .withUserConfiguration(TestConfig::class.java)

    @Test
    fun `coordinator store defaults to redis`() {
        contextRunner.run { context ->
            assertNull(context.startupFailure)
            assertEquals(
                CoordinatorProperties.StoreType.REDIS,
                context.getBean(CoordinatorProperties::class.java).store.type,
            )
        }
    }

    @Test
    fun `memory store is allowed only when stream provisioning is disabled`() {
        contextRunner
            .withPropertyValues(
                "coordinator.store.type=memory",
                "coordinator.streams.provisioning-enabled=false",
            )
            .run { context ->
                assertNull(context.startupFailure)
                assertEquals(
                    CoordinatorProperties.StoreType.MEMORY,
                    context.getBean(CoordinatorProperties::class.java).store.type,
                )
            }
    }

    @Test
    fun `memory store cannot be combined with stream provisioning`() {
        contextRunner
            .withPropertyValues(
                "coordinator.store.type=memory",
                "coordinator.streams.provisioning-enabled=true",
            )
            .run { context ->
                val failure = assertNotNull(context.startupFailure)
                assertTrue(
                    failure.causes().any { cause ->
                        cause is IllegalStateException &&
                            cause.message?.contains("coordinator.store.type=memory cannot be used") == true
                    },
                )
            }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(CoordinatorProperties::class)
    @Import(CoordinatorStoreSafetyConfig::class)
    private class TestConfig
}

private fun Throwable.causes(): Sequence<Throwable> =
    generateSequence(this) { it.cause }
