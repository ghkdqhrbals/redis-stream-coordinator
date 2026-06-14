package com.redisstream.producer

import java.time.Duration

class ProducerRoutingProperties {
    var streamPrefix: String = ""
    var routingRefreshInterval: Duration = Duration.ofSeconds(30)
    var publishMaxAttempts: Int = 2
    var xadd: XAdd = XAdd()

    class XAdd {
        var maxLen: Long = 10_000_000
        var approximateTrimming: Boolean = true
    }

    companion object {
        fun producer(
            streamPrefix: String,
            configure: ProducerRoutingProperties.() -> Unit = {},
        ): ProducerRoutingProperties =
            ProducerRoutingProperties().apply {
                this.streamPrefix = streamPrefix
                configure()
            }
    }
}
