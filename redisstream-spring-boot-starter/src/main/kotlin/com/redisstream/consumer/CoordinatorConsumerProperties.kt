package com.redisstream.consumer

import java.time.Duration
import java.util.UUID

class CoordinatorConsumerProperties {
    var autoStartup: Boolean = true
    var streamPrefix: String = ""
    var consumerGroupName: String = ""
    var memberId: String = UUID.randomUUID().toString()
    var heartbeatInterval: Duration = Duration.ofSeconds(3)
    var rebalanceTimeout: Duration = Duration.ofSeconds(60)
    var runtimeMaxConcurrency: Int = 1
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
        var mode: RedisStreamAckMode = RedisStreamAckMode.AUTO
        var xackdelReferencePolicy: RedisStreamXAckDelReferencePolicy = RedisStreamXAckDelReferencePolicy.ACKED
    }

    class RedisFailureHandling {
        var mode: RedisStreamFailureMode = RedisStreamFailureMode.LEAVE_PENDING
        var xnackMode: RedisStreamXNackMode = RedisStreamXNackMode.FAIL
        var force: Boolean = false
        var retryCount: Long? = null
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
