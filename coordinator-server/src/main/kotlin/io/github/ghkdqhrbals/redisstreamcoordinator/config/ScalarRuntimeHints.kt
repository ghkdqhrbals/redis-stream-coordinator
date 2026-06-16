package io.github.ghkdqhrbals.redisstreamcoordinator.config

import com.scalar.maven.core.authentication.ScalarAuthenticationOptions
import com.scalar.maven.core.authentication.flows.AuthorizationCodeFlow
import com.scalar.maven.core.authentication.flows.ClientCredentialsFlow
import com.scalar.maven.core.authentication.flows.ImplicitFlow
import com.scalar.maven.core.authentication.flows.OAuthFlow
import com.scalar.maven.core.authentication.flows.PasswordFlow
import com.scalar.maven.core.authentication.flows.ScalarFlows
import com.scalar.maven.core.authentication.schemes.ScalarApiKeySecurityScheme
import com.scalar.maven.core.authentication.schemes.ScalarHttpSecurityScheme
import com.scalar.maven.core.authentication.schemes.ScalarOAuth2SecurityScheme
import com.scalar.maven.core.authentication.schemes.ScalarSecurityScheme
import com.scalar.maven.core.config.DefaultHttpClient
import com.scalar.maven.core.config.ScalarAgentOptions
import com.scalar.maven.core.config.ScalarServer
import com.scalar.maven.core.config.ScalarSource
import com.scalar.maven.core.internal.ScalarConfiguration
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MemberMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MetadataCorrection
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.Migration
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardConsumptionProgress
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamMetadata
import org.springframework.aot.hint.MemberCategory
import org.springframework.aot.hint.RuntimeHints
import org.springframework.aot.hint.RuntimeHintsRegistrar

class ScalarRuntimeHints : RuntimeHintsRegistrar {
    override fun registerHints(hints: RuntimeHints, classLoader: ClassLoader?) {
        scalarJacksonTypes.forEach { type ->
            hints.reflection().registerType(
                type,
                MemberCategory.INTROSPECT_PUBLIC_CONSTRUCTORS,
                MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,
                MemberCategory.INTROSPECT_PUBLIC_METHODS,
                MemberCategory.INVOKE_PUBLIC_METHODS,
                MemberCategory.DECLARED_FIELDS,
            )
        }
    }

    private companion object {
        val scalarJacksonTypes: List<Class<*>> = listOf(
            ScalarConfiguration::class.java,
            ScalarSource::class.java,
            ScalarServer::class.java,
            ScalarServer.ServerVariable::class.java,
            DefaultHttpClient::class.java,
            ScalarAgentOptions::class.java,
            ScalarAuthenticationOptions::class.java,
            ScalarSecurityScheme::class.java,
            ScalarApiKeySecurityScheme::class.java,
            ScalarHttpSecurityScheme::class.java,
            ScalarOAuth2SecurityScheme::class.java,
            ScalarFlows::class.java,
            OAuthFlow::class.java,
            AuthorizationCodeFlow::class.java,
            ClientCredentialsFlow::class.java,
            ImplicitFlow::class.java,
            PasswordFlow::class.java,
            StreamMetadata::class.java,
            GroupMetadata::class.java,
            MemberMetadata::class.java,
            Migration::class.java,
            MetadataCorrection::class.java,
            ShardId::class.java,
            ShardConsumptionProgress::class.java,
        )
    }
}
