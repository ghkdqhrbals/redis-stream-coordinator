package io.github.ghkdqhrbals.redisstreamcoordinator

import org.springframework.beans.factory.ObjectProvider
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.connection.RedisConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets

interface StreamShardProvisioner {
    /**
     * Ensures the Redis Stream keys and consumer group for a shard version exist before
     * coordinator metadata exposes that version to members or producers.
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
            ?: throw CoordinatorException(
                HttpStatus.SERVICE_UNAVAILABLE,
                "REDIS_NOT_CONFIGURED",
                "Redis Stream provisioning is enabled but Redis is not configured",
            )

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
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "REDIS_STREAM_PROVISIONING_FAILED",
                    "Failed to provision Redis Stream consumer group '$consumerGroup' for '$streamKey': ${error.message}",
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
