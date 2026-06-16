package io.github.ghkdqhrbals.redisstreamcoordinator.config

import com.scalar.maven.core.internal.ScalarConfiguration
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamMetadata
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

    @Test
    fun `coordinator metadata models are registered for native jackson serialization`() {
        val hints = RuntimeHints()

        ScalarRuntimeHints().registerHints(hints, javaClass.classLoader)

        listOf(StreamMetadata::class.java, GroupMetadata::class.java).forEach { type ->
            val typeHint = hints.reflection().getTypeHint(type)
            assertThat(typeHint)
                .describedAs("${type.simpleName} should be visible to Jackson in native images")
                .isNotNull
            assertThat(requireNotNull(typeHint).memberCategories)
                .contains(
                    MemberCategory.INTROSPECT_PUBLIC_CONSTRUCTORS,
                    MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
                    MemberCategory.INTROSPECT_PUBLIC_METHODS,
                    MemberCategory.INVOKE_PUBLIC_METHODS,
                    MemberCategory.DECLARED_FIELDS,
                )
        }
    }
}
