package io.github.ghkdqhrbals.redisstreamcoordinator

import io.lettuce.core.internal.HostAndPort
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.DefaultClientResources
import io.lettuce.core.resource.MappingSocketAddressResolver
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.function.Function

@Configuration
class RedisClientConfig(
    private val properties: CoordinatorProperties,
) {
    @Bean(destroyMethod = "shutdown")
    fun lettuceClientResources(): ClientResources {
        val mappings = properties.redisCluster.nodeMappings.associateBy {
            it.advertisedHost to it.advertisedPort
        }
        val mappingsByPort = properties.redisCluster.nodeMappings.associateBy { it.advertisedPort }
        val resolver = MappingSocketAddressResolver.create(Function<HostAndPort, HostAndPort> { advertised ->
            val mapping = mappings[advertised.hostText to advertised.port]
                ?: mappingsByPort[advertised.port]
            if (mapping == null) {
                advertised
            } else {
                HostAndPort.of(mapping.connectHost, mapping.connectPort)
            }
        })

        return DefaultClientResources.builder()
            .socketAddressResolver(resolver)
            .build()
    }
}
