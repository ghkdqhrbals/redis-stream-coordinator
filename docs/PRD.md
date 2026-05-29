# Redis Stream Coordinator Design

This document describes a Redis Stream sharding system that adapts the coordinator role from KIP-848 to Redis Stream consumer ownership. The project provides a dedicated coordinator server, a Spring Boot consumer integration module, and a Spring Boot producer routing/publishing module.

## Design Index

1. [Context, Goals, Non-Goals](prd/01-context-goals.md)
2. [Coordinator Architecture](prd/02-coordinator-architecture.md)
3. [Group Metadata and Assignment Model](prd/03-group-assignment-model.md)
4. [Stream Version Migration, Routing, and Admin API](prd/04-stream-version-migration.md)
5. [Member Data-Plane Boundary](prd/05-processing-reliability.md)
6. [Coordinator Data, Configuration, and Observability](prd/06-data-config-observability.md)
7. [MVP Scope, Tradeoffs, Risks, and Open Questions](prd/07-mvp-risks-open-questions.md)
8. [KIP-848 Implementation Coverage](prd/08-kip848-implementation-coverage.md)
9. [Coordinator API Endpoints](prd/09-api-endpoints.md)
10. [RedisStream Spring Boot Starter and Integration Contract](prd/10-redisstream-spring-boot-starter.md)
11. [Versioning and Compatibility Policy](prd/11-versioning-compatibility.md)

## Product Summary

Redis Stream Coordinator is a control-plane server that centrally manages Redis Stream shard ownership. Consumer runtime members send heartbeats to the coordinator API, report their current ownership, and receive the target assignment they should converge to. The coordinator recalculates target assignment when membership, capacity, shard count, or stream version metadata changes.

The consumer module connects Spring Boot applications to coordinator heartbeat, assignment, revocation, fencing, and optional Redis Stream polling. The producer module resolves coordinator-managed routing metadata and publishes records to the active Redis Stream shard.

The system is designed for Redis Stream workloads that need to avoid single-stream BigKey growth and distribute traffic across Redis Cluster hash slots while retaining a Kafka-like coordinator-managed rebalance model.

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
* Provide Spring Boot integration while leaving business message processing and Redis Stream acknowledgement policy under application control.
* Use at-least-once processing as the baseline. Producer retry, consumer crash, pending recovery, and resharding can create duplicate attempts.
* Do not claim exactly-once side effects. Business side effects cannot be committed atomically with Redis Stream ACK across arbitrary databases, Redis writes, HTTP calls, or external APIs.
* The same partition key can route to a different Redis Stream shard after shard count or active stream version changes.
* Version public APIs, heartbeat protocol, and Redis metadata schema explicitly.

## Success Criteria

* When a member joins, leaves, or expires, only affected shards move.
* A shard ownership handoff follows `revoke ack -> target install -> assign`.
* Coordinator monitoring APIs show group epoch, assignment epoch, member epoch, target/current assignment, revoke progress, consumer progress, and active resharding state.
* Producer routing uses the active stream version and shard count returned by the coordinator.
* Shard count changes use a next-version stream migration instead of in-place key rewriting.
* Duplicate-sensitive workloads can stop producers, drain in-flight publish attempts, perform resharding, refresh routing metadata, and resume publishing.
* Spring Boot applications can integrate by adding the starter and implementing `CoordinatorShardLifecycle` or the built-in Redis Stream polling handler.
* Minor version upgrades support N/N-1 client/server coexistence during rolling upgrades.

## Current Implementation Snapshot

Last reviewed: 2026-05-29.

Implemented:

* Spring Boot 4 / Kotlin / Java 24 Gradle multi-module project.
* `coordinator-server` with group creation, heartbeat reconciliation, sticky assignment, revoke-before-assign, member expiration, resharding, rollback, monitoring APIs, Redis-backed state mutex, ACL, audit logging, admin mutation rate limiting, and coordinator-owned Micrometer metrics.
* Memory and Redis-backed coordinator state stores with Redis Cluster-safe key layout, Redis metadata schema guards, Lua-backed aggregate/projection writes, store revision checks, and optional Redis Stream shard provisioning.
* `com.redisstream:redisstream-spring-boot-starter` with consumer heartbeat lifecycle, shard lifecycle callbacks, runtime capacity/progress reporting, optional Redis Stream polling, producer routing cache, publisher stale-cache refresh, graceful leave, and shared Redis command templates.
* Local Redis Cluster Docker Compose, AWS public Redis test profile, coordinator Dockerfile, sample consumer/publisher pods, Docker smoke workflow, manual GHCR publish workflow, and gated Redis integration tests.
* Open source documentation for testing, Docker, operations, security, contribution, and versioning.

Not yet complete:

* First public Docker image release.
* Production hardening around external deployment examples and release automation.

Detailed progress is tracked in [Implementation Status](implementation-status.md).

## Guarantee Boundaries

* Routing determinism is guaranteed only for the same routing protocol, `activeWriteVersion`, `shardCount`, and partition key.
* Shard scale-out/in changes the routing domain. The same partition key can route to different shard indexes between old and new stream versions.
* The baseline processing model is at-least-once.
* Single processing and exactly-once side effects are not guaranteed.
* Applications must handle duplicate side effects through domain-level idempotency, deduplication, unique constraints, or compensation.
