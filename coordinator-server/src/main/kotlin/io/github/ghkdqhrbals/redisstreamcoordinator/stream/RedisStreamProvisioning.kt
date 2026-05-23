package io.github.ghkdqhrbals.redisstreamcoordinator.stream

import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorError
import io.github.ghkdqhrbals.redisstreamcoordinator.api.CoordinatorException
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import org.springframework.beans.factory.ObjectProvider
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.connection.RedisConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets

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
    private val redisConnectionFactory: ObjectProvider<RedisConnectionFactory>,
) : StreamShardProvisioner {
    override fun provision(plan: RedisStreamShardProvisioningPlan) {
        if (!properties.streams.provisioningEnabled) {
            return
        }

        val factory = redisConnectionFactory.ifAvailable
            ?: throw CoordinatorException(CoordinatorError.REDIS_NOT_CONFIGURED)

        factory.connection.use { connection ->
            plan.shardKeys.forEach { shardKey ->
                ensureConsumerGroup(connection, shardKey.value, plan.consumerGroup)
            }
        }
    }

    private fun ensureConsumerGroup(connection: RedisConnection, streamKey: String, consumerGroup: String) {
        try {
            connection.streamCommands().xGroupCreate(streamKey.bytes(), consumerGroup, ReadOffset.latest(), true)
        } catch (error: DataAccessException) {
            if (!error.isBusyGroup()) {
                throw CoordinatorException(
                    CoordinatorError.REDIS_STREAM_PROVISIONING_FAILED,
                    "Failed to provision Redis Stream consumer group '$consumerGroup' for '$streamKey': ${error.message}",
                    error,
                )
            }
        }
    }

    private fun String.bytes(): ByteArray =
        toByteArray(StandardCharsets.UTF_8)

    private fun Throwable.isBusyGroup(): Boolean =
        generateSequence(this) { it.cause }
            .any { it.message?.contains("BUSYGROUP", ignoreCase = true) == true }
}
