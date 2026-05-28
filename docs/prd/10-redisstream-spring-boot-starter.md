# RedisStream Spring Boot Starter and Integration Contract

## Purpose

`com.redisstream:redisstream-spring-boot-starter` provides the runtime pieces that application developers need to connect to Redis Stream Coordinator.

The starter intentionally does not hide the processing model. Applications can use the shard lifecycle API directly or opt into the built-in Redis Stream polling adapter.

## Consumer Integration

Applications configure a coordinator endpoint and group identity:

```yaml
redis-stream-coordinator:
  consumer:
    coordinator-base-url: http://localhost:8080
    stream-prefix: orders
    consumer-group: orders-consumer
    member-name: orders-worker
    runtime-max-concurrency: 4
```

The starter:

* creates or loads a member ID,
* sends heartbeats,
* applies assignment responses,
* calls shard lifecycle callbacks,
* reports owned shards and revoke acknowledgements,
* reports runtime capacity and optional progress,
* handles fencing responses.

## Shard Lifecycle API

Applications can implement `CoordinatorShardLifecycle`:

```kotlin
@Component
class OrdersShardLifecycle : CoordinatorShardLifecycle {
    override fun onAssigned(
        shards: Set<CoordinatorShard>,
        context: CoordinatorConsumerContext,
    ) {
        // Start local workers for these shards.
    }

    override fun onRevoked(
        shards: Set<CoordinatorShard>,
        context: CoordinatorConsumerContext,
    ): Set<CoordinatorShard> {
        // Stop new reads and wait for local in-flight work to drain.
        return shards
    }
}
```

Lifecycle callbacks should be idempotent. The same assignment can be delivered more than once after retry or reconnect.

## Optional Redis Stream Polling Adapter

Applications that want the starter to perform Redis Stream reads can enable the polling adapter:

```yaml
redis-stream-coordinator:
  consumer:
    redis:
      enabled: true
      poll-batch-size: 10
      poll-timeout: 1s
      ack-mode: xackdel
```

Supported ACK modes:

| Mode | Behavior |
| --- | --- |
| `xackdel` | ACK and delete after successful handler execution |
| `xack` | ACK after successful handler execution |
| `xnack` | Do not ACK automatically; leave retry handling to application policy |

Applications provide a handler:

```kotlin
@Component
class OrdersMessageHandler : RedisStreamMessageHandler {
    override fun handle(message: ConsumedRedisStreamMessage) {
        // Execute business processing.
        // The adapter acknowledges only after this method returns successfully.
    }
}
```

## Consumer Progress Reporting

The consumer can report:

* last delivered Redis Stream ID,
* last acknowledged Redis Stream ID,
* pending count,
* in-flight count,
* shard read state.

The coordinator uses this for monitoring and drain decisions. It does not use progress as an exactly-once guarantee.

## Producer Integration

Producer applications configure coordinator routing and Redis connectivity:

```yaml
redis-stream-coordinator:
  producer:
    coordinator-base-url: http://localhost:8080
    stream-prefix: orders
    consumer-group: orders-consumer
    routing-cache-ttl: 5s
    maxlen: 100000
    approximate-trimming: true
```

Applications publish through `RedisStreamPublisher`:

```kotlin
redisStreamPublisher.publish(
    partitionKey = "order-123",
    fields = mapOf("eventId" to "evt-1", "payload" to "..."),
)
```

The publisher:

* reads producer routing metadata,
* caches metadata by `metadataVersion`,
* routes partition keys to active stream shards,
* sends XADD with configured max length policy,
* refreshes routing metadata on stale route signals,
* returns the produced stream key and Redis Stream ID.

## Redis Command Template

Redis commands should be centralized behind shared templates instead of scattered across consumer and producer classes.

Benefits:

* Redis version compatibility checks happen in one place.
* `XACKDEL`, `XACK`, `XREADGROUP`, `XADD`, and fallback behavior are easier to test.
* Producer and consumer modules use consistent serialization.
* Unsupported command paths can fail fast with clear errors.

## Redis Version Compatibility

The starter should check Redis server version before using version-specific commands. If the connected Redis version does not support a configured command, the module should either:

* fail fast with a clear configuration error, or
* use a documented fallback when safe.

Examples:

* `XACKDEL` requires Redis support for that command.
* `XACK` is the conservative fallback when delete-after-ack is not required.
* Max length trimming must match the selected Redis command behavior.

## Metrics Boundary

Shared ownership, routing, and progress metrics belong to the coordinator. Consumer and producer modules can expose local application metrics if an application chooses to add them, but the starter should avoid defining a second public metric surface for coordinator-owned state.

## Compatibility Contract

The starter must send `protocolVersion` in heartbeat requests. Coordinator servers accept a configured protocol version range so old and new clients can coexist during rolling upgrades.

Breaking changes require:

* major version bump,
* compatibility notes,
* migration guide,
* tests covering N/N-1 compatibility.
