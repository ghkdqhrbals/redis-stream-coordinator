package com.redisstream.producer

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("redis-stream-coordinator.producer")
class ProducerRoutingProperties {
    var enabled: Boolean = false
    var coordinatorBaseUrl: String = "http://localhost:8080"
    var streamPrefix: String = ""
    var consumerGroup: String = ""
    var routingRefreshInterval: Duration = Duration.ofSeconds(30)
    var username: String? = null
    var password: String? = null
}
