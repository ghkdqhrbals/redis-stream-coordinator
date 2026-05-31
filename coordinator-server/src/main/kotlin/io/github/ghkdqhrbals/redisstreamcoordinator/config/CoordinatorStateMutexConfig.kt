package io.github.ghkdqhrbals.redisstreamcoordinator.config

import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorStateMutex
import io.github.ghkdqhrbals.redisstreamcoordinator.service.LocalCoordinatorStateMutex
import io.github.ghkdqhrbals.redisstreamcoordinator.service.RedisCoordinatorMutexStore
import io.github.ghkdqhrbals.redisstreamcoordinator.service.RedisCoordinatorStateMutex
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class CoordinatorStateMutexConfig {
    @Bean(destroyMethod = "close")
    fun coordinatorStateMutex(
        properties: CoordinatorProperties,
        redisCommands: ObjectProvider<CoordinatorRedisCommands>,
    ): CoordinatorStateMutex {
        val mutex = properties.coordination.stateMutex
        if (!mutex.enabled || properties.store.type != CoordinatorProperties.StoreType.REDIS) {
            return LocalCoordinatorStateMutex
        }

        return RedisCoordinatorStateMutex(
            ownerId = properties.id,
            mutexKey = "${properties.store.keyPrefix}:state-mutex",
            mutexTtl = mutex.resolvedTtl,
            acquireTimeout = mutex.resolvedAcquireTimeout,
            retryInterval = mutex.resolvedRetryInterval,
            mutexStore = RedisCoordinatorMutexStore(redisCommands.getObject()),
        )
    }
}
