package io.github.ghkdqhrbals.redisstreamcoordinator.config

import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorStateMutex
import io.github.ghkdqhrbals.redisstreamcoordinator.service.LocalCoordinatorStateMutex
import io.github.ghkdqhrbals.redisstreamcoordinator.service.RedisCoordinatorMutexStore
import io.github.ghkdqhrbals.redisstreamcoordinator.service.RedisCoordinatorStateMutex
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate

@Configuration
class CoordinatorStateMutexConfig {
    @Bean(destroyMethod = "close")
    fun coordinatorStateMutex(
        properties: CoordinatorProperties,
        redisTemplate: ObjectProvider<StringRedisTemplate>,
    ): CoordinatorStateMutex {
        val mutex = properties.coordination.stateMutex
        if (!mutex.enabled || properties.store.type != CoordinatorProperties.StoreType.REDIS) {
            return LocalCoordinatorStateMutex
        }

        return RedisCoordinatorStateMutex(
            ownerId = properties.id,
            mutexKey = "${properties.store.keyPrefix}:state-mutex",
            mutexTtl = mutex.ttl,
            acquireTimeout = mutex.acquireTimeout,
            retryInterval = mutex.retryInterval,
            mutexStore = RedisCoordinatorMutexStore(redisTemplate.getObject()),
        )
    }
}
