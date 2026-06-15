package io.github.ghkdqhrbals.redisstreamcoordinator.config

import org.springframework.beans.factory.InitializingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class CoordinatorStoreSafetyConfig {
    @Bean
    fun coordinatorStoreSafetyValidator(properties: CoordinatorProperties): CoordinatorStoreSafetyValidator =
        CoordinatorStoreSafetyValidator(properties)
}

class CoordinatorStoreSafetyValidator(
    private val properties: CoordinatorProperties,
) : InitializingBean {
    override fun afterPropertiesSet() {
        if (properties.store.type == CoordinatorProperties.StoreType.MEMORY && properties.streams.provisioningEnabled) {
            throw IllegalStateException(
                "coordinator.store.type=memory cannot be used with coordinator.streams.provisioning-enabled=true; " +
                    "use coordinator.store.type=redis so coordinator metadata and Redis Stream shards share durable state.",
            )
        }
    }
}
