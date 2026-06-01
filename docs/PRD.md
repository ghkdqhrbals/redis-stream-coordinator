# Redis Stream Coordinator Design

Redis Stream Coordinator adapts the coordinator-managed consumer rebalance idea from Kafka KIP-848 to Redis Stream sharding. The project provides a dedicated coordinator server, a Spring Boot consumer integration module, a Spring Boot producer routing/publishing module, and monitoring/API documentation for operating the system.

The reason for this module is practical: Redis Stream is useful as a lightweight log, but a single stream key can become a BigKey and a single Redis Cluster hash-slot hotspot. Redis itself does not provide a broker-side coordinator that owns shard assignment, membership, producer routing metadata, revoke-before-assign handoff, and resharding protocol for a logical stream. There are also very few public references that combine Redis Stream BigKey mitigation, Redis Cluster slot distribution, and consumer ownership coordination. This project fills that gap with a reusable open-source control plane and Spring Boot integration.

## Design Index

1. [Context, Goals, Non-Goals](prd/01-context-goals.md)
2. [Coordinator Architecture](prd/02-coordinator-architecture.md)
3. [Group Metadata and Assignment Model](prd/03-group-assignment-model.md)
4. [Resharding, Routing, and Admin API](prd/04-resharding-routing.md)
5. [Member Data-Plane Boundary](prd/05-processing-reliability.md)
6. [Coordinator Data, Configuration, and Observability](prd/06-data-config-observability.md)
7. [MVP Scope, Tradeoffs, Risks, and Open Questions](prd/07-mvp-risks-open-questions.md)
8. [KIP-848 Implementation Coverage](prd/08-kip848-implementation-coverage.md)
9. [Coordinator API Endpoints](prd/09-api-endpoints.md)
10. [RedisStream Spring Boot Starter and Integration Contract](prd/10-redisstream-spring-boot-starter.md)
11. [Versioning and Compatibility Policy](prd/11-versioning-compatibility.md)
12. [Failure Modes and Edge Cases](prd/12-failure-modes-edge-cases.md)
13. [Scalar API Reference](../api.html)

## Why This Exists

Redis Stream applications often start with one stream key per topic. That is simple, but it concentrates stream entries, pending-entry metadata, consumer-group metadata, and write/read traffic on one Redis key. In Redis Cluster, that one key maps to one hash slot and one primary node, so adding more cluster nodes does not automatically distribute a hot stream.

The usual mitigation is to split one logical stream into many physical stream shard keys. Once that happens, the application needs a control plane: producers need the current shard layout, consumers need ownership assignment, and operators need a way to scale shard count without two consumers processing the same shard at the same time.

This project provides that missing control plane. It keeps Redis Stream as the data plane and adds coordinator-owned metadata, heartbeat-driven assignment, revoke-before-assign handoff, routing metadata for producers, and monitoring APIs for operators.

## Product Summary

Redis Stream Coordinator is a control-plane server that centrally manages Redis Stream shard ownership. Consumer runtime members send heartbeats to the coordinator API, report their current ownership, and receive the target assignment they should converge to. The coordinator recalculates target assignment when membership, capacity, shard count, or shard metadata changes.

The consumer module connects Spring Boot applications to coordinator heartbeat, assignment, revocation, fencing, and optional Redis Stream polling. The producer module resolves coordinator-managed routing metadata and publishes records to the active Redis Stream shard.

The system is designed for Redis Stream workloads that need to avoid single-stream BigKey growth and distribute traffic across Redis Cluster hash slots while retaining a coordinator-managed rebalance model.

## Core Decisions

* Use a central Group Coordinator.
* Identify a group from `{streamPrefix, consumerGroup}` in the API path instead of hard-coding group identity in coordinator YAML.
* Let the coordinator expire members whose heartbeat age exceeds `member-lease-ttl`, fence stale owners, and recalculate target assignment.
* Let member runtimes create their own UUID member IDs while the coordinator owns registration state, epochs, and fencing.
* Store shard ownership as coordinator-calculated target assignment and member-reported current assignment.
* Require revoke-before-assign: a shard is not assigned to a new live member until the previous owner has acknowledged revocation or expired.
* Run rebalance as member-level reconciliation, not a group-wide stop-the-world barrier.
* Change shard count only through the Coordinator Admin API.
* Treat consumer `maxConcurrency` as local worker capacity, not as shard count.
* Keep coordinator YAML limited to Redis connectivity, security, event-loop defaults, and operational defaults.
* Store per-group shard count and consumer concurrency policy in coordinator metadata.
* Store each group aggregate in one Redis metadata key and use store revision CAS for updates.
* If a consumer reports a higher metadata version than Redis currently stores, the coordinator repeatedly sends `SYNC_METADATA` until the consumer heartbeats with the current Redis metadata version.
* During metadata correction, consumers drain removed shards and receive `REVOKE_PENDING` until conflicting previous owners are released; only `OK` permits starting newly assigned shards.
* Provide Spring Boot integration while leaving business message processing and Redis Stream acknowledgement policy under application control.
* Use at-least-once processing as the baseline. Producer retry, consumer crash, pending recovery, and resharding can create duplicate attempts.
* Do not claim exactly-once side effects. Business side effects cannot be committed atomically with Redis Stream ACK across arbitrary databases, Redis writes, HTTP calls, or external APIs.
* The same partition key can route to a different Redis Stream shard after shard count changes.
* Version public APIs, coordinator-module compatibility, and metadata schema explicitly.

## Success Criteria

* When a member joins, leaves, or expires, only affected shards move.
* A shard ownership handoff follows `revoke ack -> target install -> assign`.
* Coordinator monitoring APIs show group epoch, assignment epoch, member epoch, target/current assignment, revoke progress, consumer progress, and active resharding state.
* Producer routing uses the shard count returned by the coordinator.
* Shard count changes use coordinator-managed resharding instead of in-place key rewriting.
* Duplicate-sensitive workloads can stop producers, drain in-flight publish attempts, perform resharding, refresh routing metadata, and resume publishing.
* Spring Boot applications can integrate by adding the starter and implementing `CoordinatorShardLifecycle` or the built-in Redis Stream polling handler.
* Minor version upgrades support N/N-1 client/server coexistence during rolling upgrades.

## Guarantee Boundaries

* Routing determinism is guaranteed only for the same routing protocol, `shardCount`, and partition key.
* Shard scale-out/in changes the routing domain. The same partition key can route to different shard indexes between old and new shard counts.
* The baseline processing model is at-least-once.
* Single processing and exactly-once side effects are not guaranteed.
* Applications must handle duplicate side effects through domain-level idempotency, deduplication, unique constraints, or compensation.
