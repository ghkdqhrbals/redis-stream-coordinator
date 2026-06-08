# RedisStream Spring Boot Starter and Integration Contract

## Purpose

`io.github.ghkdqhrbals:redisstream-spring-boot-starter` provides the runtime pieces that application developers need to connect to Redis Stream Coordinator.

The starter intentionally does not hide the processing model. Applications can use the shard lifecycle API directly or opt into the built-in Redis Stream polling adapter.

## Public Integration Surface

Application modules should only depend on the Spring Boot starter package. Coordinator server internals such as mutex guards, aspects, and critical-section annotations are implementation details and are not part of the public API.

The main application-facing functions are:

| API | Purpose |
| --- | --- |
| `@StreamConfiguration` | Declare class-level defaults for coordinator-managed listener methods, including polling and poller thread pool size. |
| `@StreamListener` | Mark a business handler method as a listener endpoint and set endpoint identity, stream prefix, group ID, startup, and concurrency. |
| `CoordinatorConsumerProperties.consumer(...)` | Register a managed consumer for one `{streamPrefix, consumerGroupName}` pair. |
| `ProducerRoutingProperties.producer(...)` | Register a producer routing cache for one `{streamPrefix, consumerGroupName}` pair. |
| `RedisStreamPublisher.publish(...)` | Route a partition key and append a record to the active Redis Stream shard. |
| `CoordinatorShardLifecycle` | Receive shard assign/revoke callbacks when the application owns its own read workers. |
| `RedisStreamMessageHandler` | Let the built-in polling adapter execute business handling; application code explicitly ACKs, ACKDELs, or NACKs. |

Minimal annotation-based consumer registration:

```kotlin
@StreamConfiguration
class OrdersConsumer {
    @StreamListener(
        id = "orders-consumer-a",
        streamPrefix = "orders",
        groupId = "orders-consumer",
        concurrency = "4",
    )
    fun consume(message: ConsumedRedisStreamMessage) {
        // Business processing.
        message.ack()
    }
}
```

For annotation listeners, `concurrency = "4"` creates four logical coordinator members in the same application process. Each logical member has its own `memberId`, heartbeat loop, Redis consumer name, assignment state, and shard ownership. The starter derives the base member id from pod IP context and appends concurrency suffixes such as `-m0`, `-m1`, `-m2`, and `-m3`. This is the Kafka-like listener concurrency model.

The annotation listener always uses this logical-member split model. The starter does not expose a split toggle, and there is no supported mode where `concurrency = "4"` means one coordinator member with four local pollers.

Shard ownership and member concurrency are not the same thing. The coordinator assigns each shard to exactly one live member, while a single member can own many shards. Listener concurrency increases the number of logical members participating in assignment; it does not change the rule that a shard has one live owner.

For annotation-based consumers, the starter creates the runtime `memberId` automatically from `POD_IP`, local host address, or hostname. In Kubernetes, expose `status.podIP` as `POD_IP` with the Downward API. `id` is the listener endpoint identity, not the coordinator member ID.

Heartbeat interval and rebalance timeout default to the shared coordination version timing. `rebalanceTimeout` is the maximum time the coordinator waits for this member to drain revoked shards before it may fence the member and reassign those shards.

Minimal bean-based consumer and producer registration:

```kotlin
@Bean
fun ordersConsumer(): CoordinatorConsumerProperties =
    CoordinatorConsumerProperties.consumer("orders", "orders-consumer") {
        runtimeMaxConcurrency = 4
    }

@Bean
fun ordersProducer(): ProducerRoutingProperties =
    ProducerRoutingProperties.producer("orders", "orders-consumer") {
        xadd.maxLen = 100_000
    }
```

In the bean-based path, `runtimeMaxConcurrency = 4` is local worker capacity for one coordinator member. It does not create four member IDs. Applications that want Kafka-like member fan-out should use `@StreamListener(concurrency = "N")` or explicitly register multiple managed consumers with distinct member identities.

Minimal publish call:

```kotlin
redisStreamPublisher.publish(
    partitionKey = "order-123",
    fields = mapOf("eventId" to "evt-1", "payload" to payload),
)
```

## Consumer Integration

Applications configure only the coordinator endpoint through official starter YAML:

```yaml
redis-stream-coordinator:
  coordinator-base-url: http://localhost:8080
```

Consumer group identity and runtime behavior are code-defined so applications can create multiple consumers, derive settings from their own config system, and version the integration contract explicitly:

```kotlin
@Configuration(proxyBeanMethods = false)
class OrdersConsumerConfiguration {
    @Bean
    fun ordersConsumerProperties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties.consumer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
        ) {
            runtimeMaxConcurrency = 4
            heartbeatInterval = Duration.ofSeconds(3)
            rebalanceTimeout = Duration.ofSeconds(60)
        }
}
```

`consumerGroupName` is the Redis Stream consumer group name. The starter does not expose `member-name` as a YAML property; heartbeat compatibility fields are derived internally from `consumerGroupName`.

When a managed consumer bean is created, the starter fetches coordinator routing metadata for the configured `streamPrefix` and `consumerGroupName`. If the group does not exist, `shardCount` is zero, or active shard metadata is incomplete, application startup fails immediately.

The starter:

* creates or loads a member ID,
* sends heartbeats,
* applies assignment responses,
* calls shard lifecycle callbacks,
* reports owned shards and revoke acknowledgements,
* reports runtime capacity and optional progress,
* handles fencing responses.

Heartbeat status handling:

| Status | Starter behavior |
| --- | --- |
| `OK` | Applies assignment and starts newly assigned shards. |
| `SYNC_METADATA` | Updates local metadata version, keeps only currently owned shards that the coordinator still marks assigned, revokes the rest, and does not start new shards. |
| `REVOKE_PENDING` | Continues revoke/drain and keeps new shards pending until a later `OK`. |
| `RETRY` | Keeps local ownership and retries with full state. |
| `UNKNOWN_MEMBER_ID` / `FENCED_MEMBER_EPOCH` | Stops local ownership and rejoins. |

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

Applications that want the starter to perform Redis Stream reads provide a `RedisStreamMessageHandler` bean and set polling options in code:

```kotlin
@Configuration(proxyBeanMethods = false)
class OrdersPollingConfiguration {
    @Bean
    fun ordersConsumerProperties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties.consumer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
        ) {
            redis.pollBatchSize = 10
            redis.pollTimeout = Duration.ofSeconds(1)
        }
}
```

The polling adapter does not auto-commit records. Handler code must call `message.ack()` after the business side effect succeeds, `message.ackDel()` when the application also wants Redis to delete the stream entry, or `message.nack()` when the application wants to use Redis XNACK.

Polling semantics:

* coordinator ownership is per shard; at most one live member may read a shard,
* listener concurrency is the number of logical members created inside the application process,
* bean-based `runtimeMaxConcurrency` is local worker capacity for a single logical member,
* when one member owns more than one shard, its workers round-robin across owned shards,
* later shard indexes must continue to be polled; the adapter must not permanently starve shards simply because one member owns more shards than its worker count,
* the built-in adapter keeps one active poll per shard to preserve shard-local ordering and avoid duplicate local reads.

Applications provide a handler:

```kotlin
@Component
class OrdersMessageHandler : RedisStreamMessageHandler {
    override fun handle(message: ConsumedRedisStreamMessage) {
        // Execute business processing.
        message.ack()
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

Producer applications also use only the shared coordinator endpoint in YAML. Routing identity and producer behavior are code-defined:

```kotlin
@Configuration(proxyBeanMethods = false)
class OrdersProducerConfiguration {
    @Bean("ordersStreamProducer")
    fun ordersStreamProducer(
        coordinatorClient: CoordinatorClient,
        redisConnectionFactory: RedisConnectionFactory,
    ): StreamProducer =
        StreamProducer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
            client = coordinatorClient,
            redisConnectionFactory = redisConnectionFactory,
            routingRefreshInterval = Duration.ofSeconds(5),
            publishMaxAttempts = 2,
            xadd = RedisStreamXAddConfiguration(
                maxLen = 100_000,
                approximateTrimming = true,
            ),
        )
}
```

Applications can define multiple `StreamProducer` beans in the same process. Each producer bean owns its own stream prefix, consumer group name, routing cache, XADD options, and publish retry settings. Application services must inject the required producer by bean name or `@Qualifier`; relying on a single unqualified producer is only appropriate for applications with one producer.

When a `StreamProducer` bean is created, it performs the same initial metadata validation and seeds the local routing cache. Missing prefix/group shard metadata is a startup error, not a first-publish error.

The producer does not send heartbeats. Shard additions and shard-count changes reach producers through periodic routing metadata refresh. `routingRefreshInterval` bounds normal propagation delay; the routing cache lease bounds how long a producer may keep publishing without a successful coordinator refresh. If the cache lease expires and refresh still fails, publish must fail closed instead of using stale routing indefinitely.

Applications publish through the selected `StreamProducer`:

```kotlin
ordersStreamProducer.publish(
    partitionKey = "order-123",
    fields = mapOf("eventId" to "evt-1", "payload" to "..."),
)
```

The publisher:

* reads producer routing metadata,
* caches metadata by `metadataVersion`,
* routes partition keys to active stream shards,
* sends `XADD NOMKSTREAM` with configured max length policy,
* refreshes routing metadata on stale route signals,
* returns the produced stream key and Redis Stream ID.

`NOMKSTREAM` is the default producer safety guard for scale-in. If a producer has stale routing metadata and targets a shard key that was removed, Redis must not recreate that stream key. The publish attempt fails, the routing cache is invalidated, and the default second attempt fetches fresh routing metadata before recalculating the target shard.

## Redis Command Template

Redis commands should be centralized behind shared templates instead of scattered across consumer and producer classes.

Benefits:

* Redis version compatibility checks happen in one place.
* `XACKDEL`, `XACK`, `XREADGROUP`, `XADD NOMKSTREAM`, and fallback behavior are easier to test.
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

The starter sends its module-defined `protocolVersion` in heartbeat requests. That field represents the coordinator-module coordination version, not a heartbeat-only version. Coordinator servers accept the module-defined supported coordination version range so old and new clients can coexist during rolling upgrades. Applications must not configure coordination versions directly.

The starter also exposes coordination version release lifecycle metadata. Each coordination version declares the semantic release that introduced it, the earliest release it is guaranteed through, and optional deprecation/removal releases.

Breaking changes require:

* major version bump,
* compatibility notes,
* migration guide,
* tests covering N/N-1 compatibility.
