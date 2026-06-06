# Redis Stream Coordinator Design

Redis Stream Coordinator는 Kafka KIP-848의 coordinator-managed rebalance 개념을 Redis Stream sharding에 맞게 재설계한 프로젝트이다. 이 프로젝트는 coordinator 전용 서버, Spring Boot consumer 통합 모듈, Spring Boot producer routing/publishing 모듈, 운영용 monitoring/API 문서를 함께 제공한다.

이 모듈을 만든 이유는 명확하다. Redis Stream은 가벼운 append-only log로 쓰기 좋지만, 하나의 stream key는 BigKey가 될 수 있고 Redis Cluster에서도 하나의 hash slot과 하나의 primary node에 트래픽이 몰릴 수 있다. Redis 자체에는 logical stream을 여러 physical shard stream key로 나눈 뒤 producer routing metadata, consumer membership, shard ownership, revoke-before-assign handoff, resharding protocol을 중앙에서 관리하는 broker-side coordinator가 없다. Redis Stream BigKey 문제와 Redis Cluster 균등 분산, consumer ownership coordination을 함께 다루는 공개 레퍼런스도 거의 없다. 이 프로젝트는 그 공백을 재사용 가능한 오픈소스 control plane과 Spring Boot integration으로 제공하기 위해 만들어졌다.

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
13. [Terraform and GitOps Governance](prd/13-terraform-governance.md)
14. [Edge Case Q&A](prd/14-edge-case-qna.md)
15. [Scalar API Reference](../api.html)

## 왜 이 프로젝트가 필요한가

Redis Stream 애플리케이션은 보통 topic 하나를 stream key 하나로 시작한다. 구조는 단순하지만 stream entry, pending-entry metadata, consumer-group metadata, read/write traffic이 하나의 Redis key에 집중된다. Redis Cluster에서도 하나의 key는 하나의 hash slot에 매핑되므로 cluster node를 늘려도 hot stream이 자동으로 분산되지 않는다.

이를 완화하려면 하나의 logical stream을 여러 physical shard stream key로 나누어야 한다. 그 순간 producer는 현재 shard layout을 알아야 하고, consumer는 어떤 shard를 읽어야 하는지 ownership assignment가 필요하며, operator는 같은 shard를 두 consumer가 동시에 처리하지 않도록 scale-out/in을 관리해야 한다.

이 프로젝트는 Redis Stream을 data plane으로 유지하면서, coordinator-owned metadata, heartbeat 기반 assignment, revoke-before-assign handoff, producer routing metadata, operator monitoring API를 제공한다.

## Product Summary

Redis Stream Coordinator는 Redis Stream shard ownership을 중앙에서 관리하는 control-plane 서버이다. 각 consumer runtime member는 coordinator API로 heartbeat를 보내 현재 상태를 보고하고, coordinator는 group metadata 변화에 따라 target assignment를 계산한다. member는 target assignment에 독립적으로 수렴하며, coordinator는 revoke가 완료되기 전 같은 shard를 다른 member에게 assign하지 않는다.

Consumer 모듈은 heartbeat, assignment, revoke, fencing, optional Redis Stream polling을 애플리케이션에 연결한다. Producer 모듈은 coordinator의 shard count과 shard routing metadata를 사용해 Redis Stream shard로 publish한다.

## Core Decisions

* 중앙 Group Coordinator를 둔다.
* coordinator는 API 요청의 `{streamPrefix, consumerGroup}`으로 group을 식별하고, application YAML에 group을 고정 설정하지 않는다.
* coordinator는 tick마다 member heartbeat를 확인하고, `member-lease-ttl`을 넘긴 member를 `EXPIRED`로 fencing한 뒤 target assignment를 다시 계산한다.
* member identity는 member runtime이 pod IP context에서 만든 `memberId`를 사용하되, coordinator가 등록/epoch/fencing 상태를 관리한다.
* Annotation 기반 consumer에서 `@StreamListener(concurrency = "N")`은 같은 애플리케이션 프로세스 안에 N개의 논리 coordinator member를 만든다는 뜻이다. 각 논리 member는 pod IP 기반 `memberId`와 concurrency suffix, heartbeat loop, Redis consumer name, assignment state, shard ownership을 가진다.
* shard assignment는 sticky partition 방식으로 계산하고 target assignment로 저장한다.
* member는 heartbeat request로 owned shard와 revoke ack를 보고하고, coordinator는 heartbeat response로 assigned/pending shard assignment와 fencing status를 내려보낸다.
* rebalance는 group-wide stop-the-world barrier가 아니라 member별 reconciliation loop로 진행한다.
* shard count 변경은 member startup YAML sync가 아니라 Coordinator Admin API로만 요청하고, coordinator가 next resharding으로 처리한다.
* Bean 기반 consumer의 `runtimeMaxConcurrency`는 하나의 coordinator member 내부 consumer worker 수이다. 이는 shard count도 아니고 annotation listener의 member count도 아니다.
* coordinator config는 Redis 접속 정보, Basic Auth admin 계정, control-plane default만 가진다.
* stream/group별 shard count와 consumer concurrency 개별 설정은 Admin API로 저장한다.
* 각 group aggregate는 Redis의 단일 metadata key에 저장하고 store revision CAS로 update한다.
* Consumer가 Redis 현재 metadata보다 높은 version을 heartbeat로 보고하면 coordinator는 consumer가 현재 Redis metadata version으로 heartbeat할 때까지 `SYNC_METADATA`를 반복 응답한다.
* Metadata correction 중 consumer는 제거된 shard를 drain하고, 충돌 이전 owner가 release될 때까지 `REVOKE_PENDING`을 받는다. 신규 assigned shard read는 `OK`에서만 시작한다.
* consumer runtime integration은 Spring Boot starter로 제공하되, 실제 message 처리와 Redis Stream read/ack 정책은 application이 구현하는 shard lifecycle interface 뒤에 둔다.
* 처리 보장 기본 기조는 at-least-once이다. Redis Stream delivery, consumer handler execution, producer retry, pending recovery는 중복 시도를 만들 수 있다.
* 단일 처리 보장은 제공하지 않는다. 실제 비즈니스 로직은 DB, Redis, HTTP, 외부 API 등 여러 side effect가 얽힐 수 있고, 이들을 Redis Stream ACK와 하나의 원자적 transaction으로 묶을 수 없다.
* 동일 partition key도 shard count가 바뀌면 다른 Redis Stream shard로 route될 수 있다. 중복에 민감한 workload는 shard scale-out/in 중 in-flight produce와 retry가 없도록 producer를 먼저 멈추고 drain해야 한다.
* 공개 API, coordinator-module compatibility version, metadata schema는 명시적으로 versioning하고, coordination version은 모듈이 정의한 지원 범위와 release lifecycle metadata로 관리한다.
* 운영 환경의 admin mutation은 가능하면 Terraform 또는 GitOps workflow로 관리한다. Terraform은 desired state와 승인 이력을 남기고, coordinator audit log는 실제 API 호출, 실패한 요청, forbidden 요청, request id, caller identity, request body fingerprint를 남기는 runtime evidence로 유지한다.

## Success Criteria

* 새 member join/leave 시 변경된 shard만 revoke/assign되고, 영향 없는 member는 계속 consume한다.
* shard owner handoff는 `revoke ack -> target install -> assign` 순서를 지킨다.
* shard count와 consumer worker `maxConcurrency` 변경은 coordinator를 통해서만 반영된다.
* at-least-once producer workload는 shard count 변경 시 갱신된 routing metadata로 online 전환할 수 있다.
* 중복에 민감한 workload는 shard count 변경 전 producer quiescence를 완료하고, scale 이후 갱신된 routing metadata로 produce를 재개한다.
* 제거 대상 shard는 member가 보고한 drain progress가 완료된 뒤 handoff 가능 상태가 된다.
* operator는 monitoring API로 group epoch, assignment epoch, member epoch, target/current assignment, revoke progress, migration progress를 확인할 수 있다.
* application developer는 starter dependency와 `@StreamListener`, `CoordinatorShardLifecycle`, built-in Redis Stream polling handler 중 하나로 coordinator heartbeat, assignment, revoke, fencing 처리를 연동할 수 있다.
* Annotation listener의 `concurrency = 4`는 하나의 member가 local poller 네 개를 가지는 모드가 아니라, 네 개의 member ID로 join하는 모드이다.
* minor 버전 업그레이드는 N/N-1 client와 server가 동시에 동작하는 rolling upgrade를 지원한다.

## Guarantee Boundaries

* Routing determinism은 같은 producer-routing protocol, 같은 `shardCount`, 같은 partition key 안에서만 보장한다.
* Shard scale-out/in은 routing domain을 바꾼다. 같은 partition key라도 shard count 변경 전후에는 서로 다른 shard index로 route될 수 있다.
* `targetShardCount=0`을 포함한 모든 scale-in은 Redis-level drain 증거로 finalize된다. Live member가 모두 expire되어 revoke ack를 보낼 consumer가 없으면 coordinator는 consumer-level revoke를 생략하고, 제거 대상 shard stream을 consume하는 모든 Redis consumer group이 `pending=0`과 known `lag=0`을 보고한 뒤에만 retired 처리한다.
* 기본 처리 모델은 at-least-once이다. 같은 business event가 여러 번 전달되거나 여러 번 처리 시도될 수 있다.
* 단일 처리 또는 단일 side effect 반영은 보장하지 않는다. 여러 비즈니스 side effect를 Redis Stream ACK와 함께 원자적으로 commit할 수 없기 때문이다.
* 중복 side effect 방지는 application domain의 idempotency, deduplication, unique constraint, compensation 정책으로 처리해야 한다.
* Shard exclusivity는 live coordinator member 기준이다. 하나의 shard는 하나의 live owner만 가질 수 있지만, 하나의 member가 여러 shard를 소유할 수 있다. 이 경우 built-in polling adapter는 owned shard를 순회해서 뒤쪽 shard가 굶지 않도록 해야 한다.
