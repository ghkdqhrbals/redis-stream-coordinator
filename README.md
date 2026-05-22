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

## Modules

* `coordinator-server`: Spring Boot control-plane server for group metadata, heartbeat, assignment, migration, monitoring, Redis-backed state, and optional Redis Stream shard provisioning.
* `consumer-spring-boot-starter`: Spring Boot starter that applications can add to join a coordinator group, send heartbeats, receive assignment changes, and implement shard lifecycle callbacks.

## Consumer Integration

Applications implement `CoordinatorShardLifecycle` and keep ownership of actual Redis Stream reads, handler execution, `XACK`, retry, DLQ, and idempotency.

```kotlin
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

```yaml
redis-stream-coordinator:
  consumer:
    coordinator-base-url: http://localhost:8080
    stream-prefix: orders
    consumer-group: orders-consumer
    member-name: orders-worker
    runtime-max-concurrency: 4
```

## Documentation

* [Implementation Status](docs/implementation-status.md)
* [IntelliJ Setup](docs/intellij-setup.md)

## Current Status

This repository now includes an early Spring Boot/Kotlin coordinator server module and a consumer Spring Boot starter. The current implementation provides the control-plane HTTP API, in-memory coordination, optional Redis-backed group metadata persistence, local Redis Cluster Docker Compose, consumer heartbeat integration, and a Codex review workflow.

## License

See [LICENSE](LICENSE).
