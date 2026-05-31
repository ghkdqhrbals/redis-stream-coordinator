package com.redisstream.producer

import java.time.Duration

class ProducerRoutingProperties {
    var streamPrefix: String = ""
    var consumerGroupName: String = ""
    var routingRefreshInterval: Duration = Duration.ofSeconds(30)
    var publishMaxAttempts: Int = 1
    var xadd: XAdd = XAdd()

    @Deprecated(
        message = "Use consumerGroupName. Producer routing settings are now code-defined, not YAML-bound.",
        replaceWith = ReplaceWith("consumerGroupName"),
    )
    var consumerGroup: String
        get() = consumerGroupName
        set(value) {
            consumerGroupName = value
        }

    class XAdd {
        var maxLen: Long = 10_000_000
        var approximateTrimming: Boolean = true
    }

    companion object {
        fun producer(
            streamPrefix: String,
            consumerGroupName: String,
            configure: ProducerRoutingProperties.() -> Unit = {},
        ): ProducerRoutingProperties =
            ProducerRoutingProperties().apply {
                this.streamPrefix = streamPrefix
                this.consumerGroupName = consumerGroupName
                configure()
            }
    }
}
