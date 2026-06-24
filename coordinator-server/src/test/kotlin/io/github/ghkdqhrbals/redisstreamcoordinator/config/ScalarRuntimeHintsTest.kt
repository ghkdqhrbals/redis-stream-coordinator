package io.github.ghkdqhrbals.redisstreamcoordinator.config

import com.scalar.maven.core.internal.ScalarConfiguration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.aot.hint.MemberCategory
import org.springframework.aot.hint.RuntimeHints

class ScalarRuntimeHintsTest {
    @Test
    fun `scalar configuration is registered for native jackson serialization`() {
        val hints = RuntimeHints()

        ScalarRuntimeHints().registerHints(hints, javaClass.classLoader)

        val scalarConfigurationHint = hints.reflection().getTypeHint(ScalarConfiguration::class.java)
        assertThat(scalarConfigurationHint).isNotNull
        assertThat(requireNotNull(scalarConfigurationHint).memberCategories)
            .contains(
                MemberCategory.INTROSPECT_PUBLIC_METHODS,
                MemberCategory.INVOKE_PUBLIC_METHODS,
            )
    }
}
