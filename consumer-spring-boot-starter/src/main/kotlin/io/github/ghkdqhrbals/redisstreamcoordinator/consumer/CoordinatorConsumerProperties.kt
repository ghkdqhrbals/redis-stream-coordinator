package io.github.ghkdqhrbals.redisstreamcoordinator.consumer

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration
import java.util.UUID

@ConfigurationProperties("redis-stream-coordinator.consumer")
class CoordinatorConsumerProperties {
    var enabled: Boolean = true
    var autoStartup: Boolean = true
    var coordinatorBaseUrl: String = "http://localhost:8080"
    var streamPrefix: String = ""
    var consumerGroup: String = ""
    var memberId: String = UUID.randomUUID().toString()
    var memberName: String = "redis-stream-consumer"
    var protocolVersion: Int = 1
    var heartbeatInterval: Duration = Duration.ofSeconds(3)
    var rebalanceTimeout: Duration = Duration.ofSeconds(60)
    var runtimeMaxConcurrency: Int = 1
    var username: String? = null
    var password: String? = null
}
