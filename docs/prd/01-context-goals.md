# Context, Goals, Non-Goals

## Context

Redis Stream stores messages under stream keys. A single hot stream key can grow into a BigKey and, in Redis Cluster, belongs to exactly one hash slot. That limits write distribution, read distribution, and operational isolation unless the application splits traffic across multiple stream keys.

Application-level sharding solves the storage and distribution problem, but it creates a coordination problem:

* producers need deterministic routing metadata,
* consumers need shard ownership assignment,
* shard owners need a safe revoke/assign protocol,
* shard count changes need a versioned migration path,
* operators need observability and rollback points.

Redis Stream Coordinator fills this gap by adapting the coordinator-managed rebalance model from KIP-848 to Redis Stream shard ownership.

## Goals

* Reduce Redis Stream BigKey risk by splitting traffic into multiple shard stream keys.
* Support Redis Cluster-friendly key distribution.
* Provide a dedicated coordinator server as the source of truth for group metadata, stream versions, and shard assignment.
* Provide Spring Boot modules for consumer integration and producer routing/publishing.
* Preserve sticky shard ownership when possible so rebalances move only the shards that need to move.
* Enforce revoke-before-assign for live members.
* Support member join, graceful leave, expiration, and rejoin.
* Support shard count changes through versioned stream migration.
* Provide monitoring APIs and metrics for ownership, epochs, member liveness, consumer progress, producer routing, and resharding.
* Make open source operation practical with Docker, sample pods, security defaults, tests, and compatibility policy.

## Non-Goals

* Kafka protocol compatibility.
* A Redis replacement for Kafka broker storage.
* Global message ordering across shards.
* Exactly-once processing or exactly-once business side effects.
* Distributed transactions across Redis Stream ACK and arbitrary application databases or APIs.
* Automatic global event-id deduplication across all stream versions and shards.
* Running the coordinator as embedded application logic inside every consumer.
* Letting member startup YAML mutate server-side shard count or group metadata.

## Assumptions

* Applications can choose a stable partition key for producer routing.
* Duplicate delivery is acceptable at the infrastructure layer and must be handled by application idempotency where needed.
* Operators can run the coordinator server with access to a durable metadata database.
* Redis Cluster is used for the Redis Stream data plane. Redis-backed coordinator metadata is allowed only for development, tests, or non-critical deployments.
* Redis Stream data-plane reads, handler retries, DLQ policy, and ACK policy remain application-owned unless the optional starter polling adapter is enabled.

## User-Facing Modules

* `coordinator-server`: dedicated control-plane server.
* `redisstream-spring-boot-starter`: Spring Boot integration for consumers and producers.
* `samples:consumer-pod`: runnable consumer pod sample.
* `samples:publisher-pod`: runnable publisher pod sample.

## Operating Model

1. An operator creates a group through the Coordinator Admin API.
2. Producers fetch routing metadata and publish to the active shard stream.
3. Consumers join through heartbeat and report current ownership.
4. The coordinator calculates target assignment and returns it in heartbeat responses.
5. Consumers revoke removed shards, start newly assigned shards, and keep heartbeating progress.
6. Operators monitor group state and trigger shard count migration when needed.
