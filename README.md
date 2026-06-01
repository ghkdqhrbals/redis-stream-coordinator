# Redis Stream Coordinator

Redis Stream Coordinator is a design project for managing Redis Stream sharding and consumer group ownership through a central coordinator.

## Why This Exists

Redis Stream stores messages under stream keys. As traffic grows, a single stream key can easily become a BigKey. Even when Redis Cluster is used, one stream key belongs to one hash slot, so the load cannot be evenly distributed across cluster nodes unless the stream is split into multiple keys.

In practice, solving this requires application-level sharding: splitting stream keys, routing producer writes, assigning shard ownership to consumers, and safely changing shard counts over time. However, there are very few public references for managing custom Redis Stream sharding specifically to avoid BigKey issues and achieve even distribution in Redis Cluster.

This project was created to fill that gap. It adapts the coordinator-managed rebalance ideas from Kafka KIP-848 to Redis Stream, using a Redis-backed coordinator as the source of truth for shard metadata and consumer assignments.

## Core Ideas

* Split Redis Stream data into shard keys to reduce BigKey risk.
* Design shard keys so they can be distributed evenly across Redis Cluster hash slots.
* Route producer writes using coordinator-managed shard routing metadata.
* Let consumer runtime members heartbeat to the coordinator and converge on coordinator-managed target assignments.
* Rebalance only the shards that need to move when members join, leave, expire, or when shard counts change.
* Handle shard count changes through coordinator-managed resharding instead of local in-place rewrites.

## Guarantee Boundaries

Routing is deterministic only for a fixed producer-routing metadata snapshot. If shard count changes, the same partition key can route to a different Redis Stream shard.

The project uses at-least-once processing as its baseline. Producer retries, consumer crashes, pending recovery, and shard scaling can all produce duplicate delivery or duplicate business-event attempts.

The project does not provide a single-processing guarantee. Real applications often combine DB writes, Redis writes, HTTP calls, and other side effects that cannot be committed atomically with Redis Stream ACKs. Applications that cannot tolerate duplicate effects must implement domain-level idempotency, deduplication, or compensation.

## Modules

* `coordinator-server`: Spring Boot control-plane server for group metadata, heartbeat, assignment, migration, monitoring, Redis-backed state, and optional Redis Stream shard provisioning.
* `redisstream-core`: shared coordination protocol contract, version metadata, and versioned timing defaults used by both the coordinator server and support modules.
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

For the built-in Redis Stream polling adapter, the simplest path is `@StreamConfiguration` plus `@StreamListener`:

```yaml
redis-stream-coordinator:
  coordinator-base-url: http://localhost:8080
```

```kotlin
import com.redisstream.consumer.ConsumedRedisStreamMessage
import com.redisstream.consumer.StreamConfiguration
import com.redisstream.consumer.StreamListener

@StreamConfiguration(
    pollBatchSize = 10,
    pollTimeoutMs = 1_000,
)
class OrdersConsumer {
    @StreamListener(
        id = "orders-consumer-a",
        streamPrefix = "orders",
        groupId = "orders-consumer",
        concurrency = "4",
    )
    fun consume(message: ConsumedRedisStreamMessage) {
        // Run business processing first, then explicitly commit the Redis Stream record.
        message.ack()
    }
}
```

`concurrency` is both the reported local capacity and the default worker thread count. If the application needs a specific thread pool, declare an `Executor` bean and set `executor = "beanName"` on `@StreamConfiguration` or `@StreamListener`.

For annotation-based consumers, the starter generates the runtime `memberId` automatically. `id` is the listener endpoint identity, not the coordinator member ID.

For advanced configuration, provide a `RedisStreamMessageHandler` bean and code-defined consumer settings directly:

```kotlin
import com.redisstream.consumer.ConsumedRedisStreamMessage
import com.redisstream.consumer.CoordinatorConsumerProperties
import com.redisstream.consumer.RedisStreamMessageHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class OrdersMessageHandler : RedisStreamMessageHandler {
    override fun handle(message: ConsumedRedisStreamMessage) {
        // Run business processing first, then explicitly commit the Redis Stream record.
        message.ack()
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
* [Published Scalar API Reference](https://ghkdqhrbals.github.io/redis-stream-coordinator/design-docs/latest/api.html)
* [Design PRD](docs/PRD.md)
* [Design PRD (Korean)](docs/ko/PRD.md)
* [OpenAPI Spec](docs/openapi/coordinator.v1.yaml)
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

The coordinator monitoring console is available at `http://localhost:8080/console`. Sign in with the configured coordinator Basic Auth user; the local default is `admin` / `password`.

The runtime API reference is available at `http://localhost:8080/scalar`. The published static API reference is generated from `docs/openapi/coordinator.v1.yaml`.

Run a full pod smoke stack with your configured external Redis, coordinator, two consumer pods, and one auto-publishing pod:

```bash
export AWS_REDIS_CLUSTER_NODES=3.39.42.28:6379
export AWS_REDIS_PASSWORD='your-redis-password'
docker compose -f compose.pods.yaml -p rsc-pods up -d --build
curl -sS http://localhost:18090/sample/status
curl -sS http://localhost:18081/sample/events
curl -sS http://localhost:18082/sample/events
```

The pod smoke stack also starts Prometheus and Grafana for coordinator-owned metrics:

* Prometheus: `http://localhost:9091`
* Grafana: `http://localhost:3001` (`admin` / `admin`)
* Dashboard: `Redis Stream Coordinator`

Prometheus scrapes `coordinator:8080/actuator/prometheus`. Grafana also provisions a `Coordinator API` datasource that calls coordinator monitoring APIs directly with Basic Auth managed by Grafana provisioning. The dashboard includes coordinator liveness, active consumers, total lag, pending entries, shard stream length, shard lag, heartbeat rate, member heartbeat age, epochs, revoke progress, resharding state, invariant violations, group/member/assignment/shard tables, and stream message payload tables with cursor-based pagination.

For message pagination in Grafana, use the dashboard variables:

* `Stream`, `Consumer Group`, `Shard`
* `Message Direction`
* `Message Limit`
* `Message Cursor`

The `Stream Messages API` panel displays `Next Cursor`; paste that value into `Message Cursor` and refresh the dashboard to load the next page.

Swagger UI is available for interactive local testing:

* Coordinator: `http://localhost:8080/swagger-ui.html`
* Consumer pod 1: `http://localhost:18081/swagger-ui.html`
* Consumer pod 2: `http://localhost:18082/swagger-ui.html`
* Publisher pod: `http://localhost:18090/swagger-ui.html`

Use `admin` / `password` in the coordinator Swagger Authorize dialog for protected coordinator endpoints.

## Current Status

This repository now includes an early Spring Boot/Kotlin coordinator server module and the RedisStream Spring Boot starter. The current implementation provides the control-plane HTTP API, in-memory coordination, optional Redis-backed group metadata persistence, local Redis Cluster Docker Compose, a coordinator Docker image path, a lightweight monitoring console, consumer heartbeat integration, producer publishing integration, and CI review/test workflows.

## License

See [LICENSE](LICENSE).
