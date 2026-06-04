# Redis Stream Consumer Pod Sample

This sample is a small Spring Boot server that loads `com.redisstream:redisstream-spring-boot-starter` as a real application would.

It is intended for local end-to-end testing:

* sends coordinator heartbeats
* receives assigned, pending, revoked, and fenced callbacks
* consumes Redis Stream messages from assigned shards
* exposes a small publish endpoint for smoke testing producer routing

The consumer itself is declared with the same public API that application code should use:

```kotlin
@StreamConfiguration
class ConsumerPodStreamListener {
    @StreamListener(
        id = "consumer-pod-listener",
        streamPrefix = "\${STREAM_PREFIX:create-order}",
        groupId = "\${CONSUMER_GROUP_NAME:\${CONSUMER_GROUP:demo-workers}}",
        concurrency = "\${CONSUMER_MEMBER_CONCURRENCY:\${CONSUMER_RUNTIME_MAX_CONCURRENCY:4}}",
    )
    fun consume(message: ConsumedRedisStreamMessage) {
        // sample handler
        message.ack()
    }
}
```

`CONSUMER_MEMBER_CONCURRENCY=4` creates four logical coordinator members inside this pod. `CONSUMER_RUNTIME_MAX_CONCURRENCY` is still accepted as a compatibility fallback.

The compose smoke setup also enables two `create-payment` groups:

* `payment-workers`: the regular payment consumer group.
* `payment-low-workers`: a lower-member payment consumer group using `PAYMENT_LOW_CONSUMER_MEMBER_CONCURRENCY=1` per pod.

Example:

```bash
SERVER_PORT=18081 CONSUMER_GROUP_NAME=demo-workers \
  ./gradlew :samples:consumer-pod:bootRun
```

Useful endpoints:

* `GET /sample/status`
* `GET /sample/events`
* `DELETE /sample/events`
* `POST /sample/publish`
