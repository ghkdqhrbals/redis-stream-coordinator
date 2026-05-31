# Redis Stream Coordinator

Redis Stream Coordinator is a design project for managing Redis Stream sharding and consumer group ownership through a central coordinator.

## Why This Exists

Redis Stream stores messages under stream keys. As traffic grows, a single stream key can easily become a BigKey. Even when Redis Cluster is used, one stream key belongs to one hash slot, so the load cannot be evenly distributed across cluster nodes unless the stream is split into multiple keys.

In practice, solving this requires application-level sharding: splitting stream keys, routing producer writes, assigning shard ownership to consumers, and safely changing shard counts over time. However, there are very few public references for managing custom Redis Stream sharding specifically to avoid BigKey issues and achieve even distribution in Redis Cluster.

This project was created to fill that gap. It adapts the coordinator-managed rebalance ideas from Kafka KIP-848 to Redis Stream, using a Redis-backed coordinator as the source of truth for shard metadata, stream versions, and consumer assignments.

## Core Ideas

* Split Redis Stream data into shard keys to reduce BigKey risk.
* Design shard keys so they can be distributed evenly across Redis Cluster hash slots.
* Route producer writes using active stream version and shard routing metadata.
* Let consumer runtime members heartbeat to the coordinator and converge on coordinator-managed target assignments.
* Rebalance only the shards that need to move when members join, leave, expire, or when shard counts change.
* Handle shard count changes through next-version stream migration instead of in-place resharding.

## Guarantee Boundaries

Routing is deterministic only for a fixed producer-routing metadata snapshot. If shard count or active stream version changes, the same partition key can route to a different Redis Stream shard.

The project uses at-least-once processing as its baseline. Producer retries, consumer crashes, pending recovery, and shard scaling can all produce duplicate delivery or duplicate business-event attempts.

The project does not provide a single-processing guarantee. Real applications often combine DB writes, Redis writes, HTTP calls, and other side effects that cannot be committed atomically with Redis Stream ACKs. Applications that cannot tolerate duplicate effects must implement domain-level idempotency, deduplication, or compensation.

## Modules

* `coordinator-server`: Spring Boot control-plane server for group metadata, heartbeat, assignment, migration, monitoring, Redis-backed state, and optional Redis Stream shard provisioning.
* `redisstream-spring-boot-starter`: Spring Boot starter that applications can add to join a coordinator group, send heartbeats, report runtime capacity, receive assignment changes, implement shard lifecycle callbacks, and publish through coordinator routing metadata.
* `samples:consumer-pod`: runnable Spring Boot sample that behaves like a consumer pod for local end-to-end coordinator, consumer, and publisher smoke tests.
* `samples:publisher-pod`: runnable Spring Boot sample that publishes records through coordinator-managed producer routing.

Application code should use the `com.redisstream:*` starter APIs only. Coordinator-server internals such as state mutex guards, AOP aspects, and critical-section annotations are implementation details, not public integration points.

## Versioning

The project treats compatibility as part of the public contract:

* Artifact versions follow Semantic Versioning.
* Coordinator-module compatibility is carried in heartbeat requests as `protocolVersion`.
* Coordinator and starter modules provide the supported coordination version range and release lifecycle; it is not a user YAML setting.
* Breaking HTTP API changes require a new path prefix such as `/coord/v2`.
* Redis metadata schema changes require migration notes and compatibility tests.

See [Versioning and Compatibility Policy](docs/prd/11-versioning-compatibility.md).

## Consumer Integration

Applications can implement `CoordinatorShardLifecycle` directly and keep ownership of actual Redis Stream reads, handler execution, `XACK`, retry, DLQ, and idempotency.

```kotlin
implementation("com.redisstream:redisstream-spring-boot-starter:<version>")
```

For the built-in Redis Stream polling adapter, provide a `RedisStreamMessageHandler` bean and code-defined consumer settings:

```yaml
redis-stream-coordinator:
  coordinator-base-url: http://localhost:8080
```

```kotlin
import com.redisstream.consumer.ConsumedRedisStreamMessage
import com.redisstream.consumer.CoordinatorConsumerProperties
import com.redisstream.consumer.RedisStreamAckMode
import com.redisstream.consumer.RedisStreamMessageHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class OrdersMessageHandler : RedisStreamMessageHandler {
    override fun handle(message: ConsumedRedisStreamMessage) {
        // Run business processing. The starter XACKs only after this returns successfully.
    }
}

@Configuration(proxyBeanMethods = false)
class OrdersConsumerConfiguration {
    @Bean
    fun ordersConsumerProperties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties.consumer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
        ) {
            runtimeMaxConcurrency = 4
            redis.pollBatchSize = 10
            redis.pollTimeout = Duration.ofSeconds(1)
            redis.ack.mode = RedisStreamAckMode.AUTO
        }
}
```

```kotlin
import com.redisstream.consumer.CoordinatorConsumerContext
import com.redisstream.consumer.CoordinatorShard
import com.redisstream.consumer.CoordinatorShardLifecycle
import org.springframework.stereotype.Component

@Component
class OrdersShardLifecycle : CoordinatorShardLifecycle {
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        // Start local Redis Stream workers for these shards.
    }

    override fun onRevoked(
        shards: Set<CoordinatorShard>,
        context: CoordinatorConsumerContext,
    ): Set<CoordinatorShard> {
        // Stop new reads, drain in-flight work, then return fully revoked shards.
        return shards
    }
}
```

Consumer and producer runtime settings are intentionally code-defined. The only official starter YAML property is `redis-stream-coordinator.coordinator-base-url`. Use `consumerGroupName` for the logical Redis Stream consumer group name; `member-name` is not a public YAML setting.

Producer applications can use the starter to route and publish to the active Redis Stream shard:

```kotlin
import com.redisstream.producer.ProducerRoutingProperties
import com.redisstream.producer.RedisStreamPublisher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration(proxyBeanMethods = false)
class OrdersProducerConfiguration {
    @Bean
    fun ordersProducerProperties(): ProducerRoutingProperties =
        ProducerRoutingProperties.producer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
        ) {
            routingRefreshInterval = Duration.ofSeconds(30)
            xadd.maxLen = 10_000_000
        }
}

redisStreamPublisher.publish(
    partitionKey = orderId,
    fields = mapOf("payload" to payload),
)
```

During Spring bean initialization, both managed consumers and producer routing caches validate coordinator routing metadata for the configured `streamPrefix` and `consumerGroupName`. If the coordinator group does not exist or has no active shards, application startup fails immediately instead of waiting for the first heartbeat or publish call.

## Documentation

* [Published Design Docs](https://ghkdqhrbals.github.io/redis-stream-coordinator/design-docs/latest/index.html)
* [Published Design Docs (Korean)](https://ghkdqhrbals.github.io/redis-stream-coordinator/design-docs/latest/index.html?tl=ko)
* [Design PRD](docs/PRD.md)
* [Design PRD (Korean)](docs/ko/PRD.md)
* [Implementation Status](docs/implementation-status.md)
* [Docker Guide](docs/docker.md)
* [Testing Guide](docs/testing.md)
* [Operations Runbook](docs/operations-runbook.md)
* [IntelliJ Setup](docs/intellij-setup.md)
* [Contributing](CONTRIBUTING.md)
* [Security Policy](SECURITY.md)
* [Changelog](CHANGELOG.md)

## Docker Quick Start

```bash
docker compose --profile coordinator up --build
curl -u admin:password http://localhost:8080/coord/v1/monitoring/health
```

Run a full local pod smoke stack with Redis Cluster, coordinator, two consumer pods, and one auto-publishing pod:

```bash
docker compose -f compose.pods.yaml -p rsc-pods up -d --build
curl -sS http://localhost:18090/sample/status
curl -sS http://localhost:18081/sample/events
curl -sS http://localhost:18082/sample/events
```

Swagger UI is available for interactive local testing:

* Coordinator: `http://localhost:8080/swagger-ui.html`
* Consumer pod 1: `http://localhost:18081/swagger-ui.html`
* Consumer pod 2: `http://localhost:18082/swagger-ui.html`
* Publisher pod: `http://localhost:18090/swagger-ui.html`

Use `admin` / `password` in the coordinator Swagger Authorize dialog for protected coordinator endpoints.

## Current Status

This repository now includes an early Spring Boot/Kotlin coordinator server module and the RedisStream Spring Boot starter. The current implementation provides the control-plane HTTP API, in-memory coordination, optional Redis-backed group metadata persistence, local Redis Cluster Docker Compose, a coordinator Docker image path, consumer heartbeat integration, producer publishing integration, and CI review/test workflows.

## License

See [LICENSE](LICENSE).
