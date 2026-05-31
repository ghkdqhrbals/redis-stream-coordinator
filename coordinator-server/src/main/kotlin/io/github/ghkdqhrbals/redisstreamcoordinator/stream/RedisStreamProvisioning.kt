package io.github.ghkdqhrbals.redisstreamcoordinator.stream

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorException
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.isRedisBusyGroup
import org.springframework.beans.factory.ObjectProvider
import org.springframework.dao.DataAccessException
import org.springframework.stereotype.Component

interface StreamShardProvisioner {
    /**
     * Ensures the Redis Stream keys and consumer group for a shard version exist.
     * Coordinator state claims or PREPARING migrations should be committed before this runs,
     * so failed state races cannot leave untracked stream versions behind.
     */
    fun provision(plan: RedisStreamShardProvisioningPlan)
}

object NoopStreamShardProvisioner : StreamShardProvisioner {
    override fun provision(plan: RedisStreamShardProvisioningPlan) = Unit
}

@Component
class RedisStreamShardProvisioner(
    private val properties: CoordinatorProperties,
    private val redisCommands: ObjectProvider<CoordinatorRedisCommands>,
) : StreamShardProvisioner {
    /**
     * Creates every stream key and consumer group required by a stream version.
     */
    override fun provision(plan: RedisStreamShardProvisioningPlan) {
        if (!properties.streams.provisioningEnabled) {
            return
        }

        val commands = redisCommands.ifAvailable?.takeIf { it.isConfigured() }
            ?: throw CoordinatorException(CoordinatorError.REDIS_NOT_CONFIGURED)

        plan.shardKeys.forEach { shardKey ->
            ensureConsumerGroup(commands, shardKey.value, plan.consumerGroup)
        }
    }

    /**
     * Creates one Redis consumer group and treats BUSYGROUP as an idempotent success.
     */
    private fun ensureConsumerGroup(commands: CoordinatorRedisCommands, streamKey: String, consumerGroup: String) {
        try {
            commands.xGroupCreate(streamKey, consumerGroup)
        } catch (error: DataAccessException) {
            if (!error.isRedisBusyGroup()) {
                throw CoordinatorException(
                    CoordinatorError.REDIS_STREAM_PROVISIONING_FAILED,
                    "Failed to provision Redis Stream consumer group '$consumerGroup' for '$streamKey': ${error.message}",
                    error,
                )
            }
        }
    }
}
