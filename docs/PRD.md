# Redis Stream Coordinator PRD

이 문서는 Redis Stream sharding을 KIP-848 스타일의 coordinator-managed protocol로 관리하기 위한 PRD entrypoint이다. 전체 요구사항은 lazy loading 방식으로 나누어 관리한다.

## Source

* KIP-848: [The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
* 사용자 정리 글: [Kafka KIP-848 는 왜 등장했는가](https://ghkdqhrbals.github.io/portfolios/docs/Java/51/)
* 기존 coordinatorless 설계: [`../redis-stream-sharding/PRD.md`](../redis-stream-sharding/PRD.md)
* 폴더 작업 지침: [`AGENTS.md`](AGENTS.md)

## Lazy Loading Index

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

Coordinator API control plane이 Redis Stream shard ownership을 중앙에서 관리한다. 각 runtime member는 coordinator API로 heartbeat를 보내 현재 상태를 보고하고, coordinator는 group metadata 변화에 따라 target assignment를 계산한다. member는 target assignment에 독립적으로 수렴하며, coordinator는 revoke가 완료되기 전 같은 shard를 다른 member에게 assign하지 않는다.

## Core Decisions

* 중앙 Group Coordinator를 둔다.
* coordinator는 API 요청의 `{streamPrefix, consumerGroup}`으로 group을 식별하고, application YAML에 group을 고정 설정하지 않는다.
* coordinator는 tick마다 member heartbeat를 확인하고, `member-lease-ttl`을 넘긴 member를 `EXPIRED`로 fencing한 뒤 target assignment를 다시 계산한다.
* member identity는 member runtime이 직접 만든 UUID를 사용하되, coordinator가 등록/epoch/fencing 상태를 관리한다.
* shard assignment는 sticky partition 방식으로 계산하고 target assignment로 저장한다.
* member는 heartbeat request로 owned shard와 revoke ack를 보고하고, coordinator는 heartbeat response로 assigned/pending shard assignment와 fencing status를 내려보낸다.
* rebalance는 group-wide stop-the-world barrier가 아니라 member별 reconciliation loop로 진행한다.
* shard count 변경은 member startup YAML sync가 아니라 Coordinator Admin API로만 요청하고, coordinator가 next stream version migration으로 처리한다.
* consumer `maxConcurrency`는 partition/shard 수가 아니라 member 내부 consumer worker 수이며, Coordinator Admin API가 저장한 server-side consumer concurrency policy로 결정한다.
* coordinator config는 Redis 접속 정보, Basic Auth admin 계정, control-plane default만 가진다.
* stream/group별 shard count와 consumer concurrency 개별 설정은 Admin API로 저장한다.
* consumer runtime integration은 Spring Boot starter로 제공하되, 실제 message 처리와 Redis Stream read/ack 정책은 application이 구현하는 shard lifecycle interface 뒤에 둔다.
* 처리 보장 기본 기조는 at-least-once이다. Redis Stream delivery, consumer handler execution, producer retry, pending recovery는 중복 시도를 만들 수 있다.
* 단일 처리 보장은 제공하지 않는다. 실제 비즈니스 로직은 DB, Redis, HTTP, 외부 API 등 여러 side effect가 얽힐 수 있고, 이들을 Redis Stream ACK와 하나의 원자적 transaction으로 묶을 수 없다.
* 동일 partition key도 shard count나 active stream version이 바뀌면 다른 Redis Stream shard로 route될 수 있다. 중복에 민감한 workload는 shard scale-out/in 중 in-flight produce와 retry가 없도록 producer를 먼저 멈추고 drain해야 한다.
* 공개 API, heartbeat protocol, Redis metadata schema는 명시적으로 versioning하고, coordinator는 rolling upgrade를 위해 구버전과 신규 버전 protocol을 범위 기반으로 동시에 수용한다.

## Success Criteria

* 새 member join/leave 시 변경된 shard만 revoke/assign되고, 영향 없는 member는 계속 consume한다.
* shard owner handoff는 `revoke ack -> target install -> assign` 순서를 지킨다.
* shard count와 consumer worker `maxConcurrency` 변경은 coordinator를 통해서만 반영된다.
* at-least-once producer workload는 shard count 변경 시 next stream version으로 online 전환할 수 있다.
* 중복에 민감한 workload는 shard count 변경 전 producer quiescence를 완료하고, scale 이후 갱신된 routing metadata로 produce를 재개한다.
* old `DRAINING` version은 member가 보고한 drain progress가 완료된 뒤 `DEPRECATED`로 전환된다.
* operator는 monitoring API로 group epoch, assignment epoch, member epoch, target/current assignment, revoke progress, migration progress를 확인할 수 있다.
* application developer는 starter dependency와 `CoordinatorShardLifecycle` 구현만으로 coordinator heartbeat, assignment, revoke, fencing 처리를 연동할 수 있다.
* minor 버전 업그레이드는 N/N-1 client와 server가 동시에 동작하는 rolling upgrade를 지원한다.

## Current Implementation Snapshot

Last reviewed: 2026-05-28.

Implemented:

* Spring Boot 4 / Kotlin / Java 24 Gradle multi-module project.
* `coordinator-server` control plane with group creation, heartbeat reconciliation, sticky assignment, revoke-before-assign, scale migration, rollback, monitoring APIs with conflict retry, consumer shard progress/member lease monitoring, producer routing request counters, scheduled coordinator event loop, Redis-backed state mutex, ACL, audit logging, admin mutation rate limiting, and coordinator-owned Micrometer metrics.
* Memory and Redis-backed coordinator state stores, including Redis Cluster-safe key layout, Redis metadata schemaVersion guard, Redis state mutex, Lua-backed aggregate/projection writes, optimistic store revision checks, and optional Redis Stream shard provisioning.
* `com.redisstream:redisstream-spring-boot-starter` with consumer heartbeat lifecycle, shard lifecycle callbacks, runtime capacity/progress reporting, opt-in Redis Stream polling adapter, producer routing cache, Redis Stream publisher stale-cache handling, graceful leave, and shared Redis command templates.
* Local three-node Redis Cluster Docker Compose, coordinator Dockerfile, Compose coordinator profile, Docker smoke workflow, manual GHCR publish workflow, and gated Redis integration tests.
* Open source contributor, testing, Docker, security, changelog, and operations documentation.

Not yet complete:

* First public Docker image release.

Detailed progress is tracked in [`implementation-status.md`](implementation-status.md).

## Guarantee Boundaries

* Routing determinism은 같은 producer-routing protocol, 같은 `activeWriteVersion`, 같은 `shardCount`, 같은 partition key 안에서만 보장한다.
* Shard scale-out/in은 routing domain을 바꾼다. 같은 partition key라도 old version과 new version에서는 서로 다른 shard index로 route될 수 있다.
* 기본 처리 모델은 at-least-once이다. 같은 business event가 여러 번 전달되거나 여러 번 처리 시도될 수 있다.
* 단일 처리 또는 단일 side effect 반영은 보장하지 않는다. 여러 비즈니스 side effect를 Redis Stream ACK와 함께 원자적으로 commit할 수 없기 때문이다.
* 중복 side effect 방지는 application domain의 idempotency, deduplication, unique constraint, compensation 정책으로 처리해야 한다.
