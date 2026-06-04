package com.redisstream.consumer

import com.redisstream.protocol.CoordinatorProtocol
import java.time.Duration

class CoordinatorConsumerProperties {
    var autoStartup: Boolean = true
    var streamPrefix: String = ""
    var consumerGroupName: String = ""
    var memberId: String = ConsumerRuntimeIdentity.defaultMemberId()
    var memberCount: Int = 1
    var heartbeatInterval: Duration = CoordinatorProtocol.DEFAULT_TIMING.heartbeatInterval
    var rebalanceTimeout: Duration = CoordinatorProtocol.DEFAULT_TIMING.rebalanceTimeout
    var runtimeMaxConcurrency: Int = 1
    var executorBeanName: String = ""
    var gracefulLeaveOnStop: Boolean = true
    var redis: RedisPolling = RedisPolling()

    @Deprecated(
        message = "Use consumerGroupName. Consumer settings are now code-defined, not YAML-bound.",
        replaceWith = ReplaceWith("consumerGroupName"),
    )
    var consumerGroup: String
        get() = consumerGroupName
        set(value) {
            consumerGroupName = value
        }

    val heartbeatMemberName: String
        get() = consumerGroupName.ifBlank { "redis-stream-consumer" }

    class RedisPolling {
        var pollBatchSize: Long = 10
        var pollTimeout: Duration = Duration.ofSeconds(1)
        var ack: RedisAcknowledgement = RedisAcknowledgement()
        var failure: RedisFailureHandling = RedisFailureHandling()
    }

    class RedisAcknowledgement {
        /**
         * Legacy acknowledgement mode kept for bean-based integrations.
         *
         * The annotation listener does not auto-commit records. Application code should call
         * `ConsumedRedisStreamMessage.ack()` or `ConsumedRedisStreamMessage.ackDel()` explicitly
         * after the business side effect succeeds.
         */
        var mode: RedisStreamAckMode = RedisStreamAckMode.AUTO
        /**
         * Redis XACKDEL reference policy used when application code calls `ackDel()`.
         *
         * `KEEPREF` keeps consumer-group references, `DELREF` removes them, and `ACKED` removes
         * references only for entries acknowledged by all groups.
         */
        var xackdelReferencePolicy: RedisStreamXAckDelReferencePolicy = RedisStreamXAckDelReferencePolicy.ACKED
    }

    class RedisFailureHandling {
        /**
         * Legacy failed-record mode kept for low-level bean integrations.
         *
         * The annotation listener does not auto-NACK on handler failure. `LEAVE_PENDING` means the
         * application leaves the record in the Redis pending entries list. `XNACK` means an
         * integration may use Redis XNACK when supported. Application code can always call
         * `ConsumedRedisStreamMessage.nack(...)` explicitly.
         */
        var mode: RedisStreamFailureMode = RedisStreamFailureMode.LEAVE_PENDING
        /**
         * Redis XNACK mode passed to Redis when application code calls `nack()`.
         *
         * `SILENT` does not fail if the record cannot be released, `FAIL` returns an error for that
         * record, and `FATAL` treats the command failure as fatal.
         */
        var xnackMode: RedisStreamXNackMode = RedisStreamXNackMode.FAIL
        /**
         * Redis XNACK force flag. When true, Redis may apply the operation even if the record is not
         * currently owned in the expected pending state.
         */
        var force: Boolean = false
        var retryCount: Long? = null
    }

    fun copyForMember(memberIndex: Int): CoordinatorConsumerProperties {
        val templateMemberId = memberId
            .ifBlank { ConsumerRuntimeIdentity.defaultMemberId() }
        val suffix = if (memberCount <= 1) "" else "-m$memberIndex"
        val nextMemberId = if (suffix.isBlank()) templateMemberId else "$templateMemberId$suffix"

        return CoordinatorConsumerProperties().apply {
            autoStartup = this@CoordinatorConsumerProperties.autoStartup
            streamPrefix = this@CoordinatorConsumerProperties.streamPrefix
            consumerGroupName = this@CoordinatorConsumerProperties.consumerGroupName
            memberId = nextMemberId
            memberCount = 1
            heartbeatInterval = this@CoordinatorConsumerProperties.heartbeatInterval
            rebalanceTimeout = this@CoordinatorConsumerProperties.rebalanceTimeout
            runtimeMaxConcurrency = this@CoordinatorConsumerProperties.runtimeMaxConcurrency
            executorBeanName = this@CoordinatorConsumerProperties.executorBeanName
            gracefulLeaveOnStop = this@CoordinatorConsumerProperties.gracefulLeaveOnStop
            redis = RedisPolling().apply {
                pollBatchSize = this@CoordinatorConsumerProperties.redis.pollBatchSize
                pollTimeout = this@CoordinatorConsumerProperties.redis.pollTimeout
                ack = RedisAcknowledgement().apply {
                    mode = this@CoordinatorConsumerProperties.redis.ack.mode
                    xackdelReferencePolicy = this@CoordinatorConsumerProperties.redis.ack.xackdelReferencePolicy
                }
                failure = RedisFailureHandling().apply {
                    mode = this@CoordinatorConsumerProperties.redis.failure.mode
                    xnackMode = this@CoordinatorConsumerProperties.redis.failure.xnackMode
                    force = this@CoordinatorConsumerProperties.redis.failure.force
                    retryCount = this@CoordinatorConsumerProperties.redis.failure.retryCount
                }
            }
        }
    }

    companion object {
        fun consumer(
            streamPrefix: String,
            consumerGroupName: String,
            configure: CoordinatorConsumerProperties.() -> Unit = {},
        ): CoordinatorConsumerProperties =
            CoordinatorConsumerProperties().apply {
                this.streamPrefix = streamPrefix
                this.consumerGroupName = consumerGroupName
                configure()
            }
    }
}

enum class RedisStreamAckMode { AUTO, XACK, XACKDEL }
enum class RedisStreamXAckDelReferencePolicy { KEEPREF, DELREF, ACKED }
enum class RedisStreamFailureMode { LEAVE_PENDING, XNACK }
enum class RedisStreamXNackMode { SILENT, FAIL, FATAL }
