package com.redisstream

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("redis-stream-coordinator")
class RedisStreamCoordinatorProperties {
    var coordinatorBaseUrl: String = "http://localhost:8080"
    var username: String = ""
    var password: String = ""
}
