# Redis Stream Coordinator Design (English)

Language: [Korean](../PRD.md)

This document describes a Redis Stream design that adapts the coordinator role from KIP-848 to Redis Stream sharding and consumer group ownership. The project provides a dedicated coordinator server, a Spring Boot consumer integration module, and a Spring Boot producer routing/publishing module.

## Product Summary

Redis Stream Coordinator is a control-plane server for centrally managing Redis Stream shard ownership. Consumer runtime members send heartbeats to the coordinator API to report their current state. The coordinator calculates target assignments from group metadata changes, and each member independently converges to its target assignment. The coordinator does not assign the same shard to another member until the previous owner has acknowledged revocation.

The consumer module connects application code to heartbeat handling, assignment updates, revocation, fencing, and optional Redis Stream polling. The producer module uses the coordinator's active stream version and shard routing metadata to publish records to Redis Stream shards.

## Design Index

1. Context, Goals, Non-Goals
2. Coordinator Architecture
3. Group Metadata and Assignment Model
4. Stream Version Migration, Routing, and Admin API
5. Member Data-Plane Boundary
6. Coordinator Data, Configuration, and Observability
7. MVP Scope, Tradeoffs, Risks, and Open Questions
8. KIP-848 Implementation Coverage
9. Coordinator API Endpoints
10. RedisStream Spring Boot Starter and Integration Contract
11. Versioning and Compatibility Policy

The detailed Korean design pages are available from the left-side navigation. This English page is the public design summary for open source users.

## Core Decisions

* Use a central Group Coordinator.
* Identify a group from the API request `{streamPrefix, consumerGroup}` instead of hard-coding group identity in the coordinator application YAML.
* On each coordinator tick, check member heartbeats, fence members that exceed `member-lease-ttl` as `EXPIRED`, and recalculate the target assignment.
* Let the member runtime generate its own UUID while the coordinator manages registration, epochs, and fencing state.
* Calculate shard assignments with sticky partition placement and persist them as target assignments.
* Let members report owned shards and revoke acknowledgements through heartbeat requests.
* Return assigned shards, pending shard assignments, and fencing status through heartbeat responses.
* Run rebalance as a member-level reconciliation loop instead of a group-wide stop-the-world barrier.
* Change shard count only through the Coordinator Admin API, not through member startup YAML synchronization.
* Treat consumer `maxConcurrency` as the number of local worker slots inside a member, not as the number of shards.
* Keep coordinator configuration limited to Redis connectivity, Basic Auth admin accounts, and control-plane defaults.
* Store per-stream/per-group shard count and consumer concurrency settings through the Admin API.
* Provide consumer runtime integration as a Spring Boot starter while leaving actual message processing and Redis Stream read/ack policy behind an application-owned shard lifecycle interface.
* Use at-least-once processing as the baseline guarantee. Redis Stream delivery, handler execution, producer retry, and pending recovery can all create duplicate attempts.
* Do not claim single-processing or exactly-once side effects. Real business logic often combines DB writes, Redis writes, HTTP calls, and external APIs that cannot be committed atomically with a Redis Stream ACK.
* The same partition key can route to a different Redis Stream shard when shard count or active stream version changes. Duplicate-sensitive workloads must stop producers and drain in-flight records before shard scale-out or scale-in.
* Version public APIs, heartbeat protocol, and Redis metadata schema explicitly. During rolling upgrades, the coordinator accepts old and new protocol versions within a configured compatibility range.

## Success Criteria

* When a member joins or leaves, only the affected shards move and unaffected members continue consuming.
* Shard ownership handoff follows `revoke ack -> target install -> assign`.
* Shard count and consumer worker `maxConcurrency` changes are applied only through the coordinator.
* At-least-once producer workloads can move to the next stream version during shard count migration.
* Duplicate-sensitive workloads can quiesce producers before shard count migration and resume publishing after refreshed routing metadata is installed.
* Old `DRAINING` stream versions move to `DEPRECATED` after members report completed drain progress.
* Operators can inspect group epoch, assignment epoch, member epoch, target/current assignment, revoke progress, and migration progress through monitoring APIs.
* Application developers can integrate heartbeat, assignment, revocation, and fencing by adding the starter dependency and implementing `CoordinatorShardLifecycle`.
* Minor version upgrades support N/N-1 client and server coexistence for rolling upgrades.

## Current Implementation Snapshot

Last reviewed: 2026-05-28.

Implemented:

* Spring Boot 4 / Kotlin / Java 24 Gradle multi-module project.
* `coordinator-server` control plane with group creation, heartbeat reconciliation, sticky assignment, revoke-before-assign, scale migration, rollback, monitoring APIs with conflict retry, consumer shard progress/member lease monitoring, producer routing request counters, scheduled coordinator event loop, Redis-backed state mutex, ACL, audit logging, admin mutation rate limiting, and coordinator-owned Micrometer metrics.
* Memory and Redis-backed coordinator state stores, including Redis Cluster-safe key layout, Redis metadata schema version guard, Redis state mutex, Lua-backed aggregate/projection writes, optimistic store revision checks, and optional Redis Stream shard provisioning.
* `com.redisstream:redisstream-spring-boot-starter` with consumer heartbeat lifecycle, shard lifecycle callbacks, runtime capacity/progress reporting, opt-in Redis Stream polling adapter, producer routing cache, Redis Stream publisher stale-cache handling, graceful leave, and shared Redis command templates.
* Local three-node Redis Cluster Docker Compose, coordinator Dockerfile, Compose coordinator profile, Docker smoke workflow, manual GHCR publish workflow, and gated Redis integration tests.
* Open source contributor, testing, Docker, security, changelog, and operations documentation.

Not yet complete:

* First public Docker image release.

Detailed progress is tracked in [`implementation-status.md`](../implementation-status.md).

## Guarantee Boundaries

* Routing determinism is guaranteed only within the same producer-routing protocol, `activeWriteVersion`, `shardCount`, and partition key.
* Shard scale-out or scale-in changes the routing domain. The same partition key can route to different shard indexes between the old and new stream versions.
* The baseline processing model is at-least-once. The same business event can be delivered or attempted more than once.
* Single processing and exactly-once side effects are not guaranteed because application side effects cannot be committed atomically with Redis Stream ACK.
* Applications must handle duplicate side effects through domain-level idempotency, deduplication, unique constraints, or compensation.
