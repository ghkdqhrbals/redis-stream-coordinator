# MVP Scope, Tradeoffs, Risks, and Open Questions

## MVP Scope

Included:

* Coordinator API control plane
* Coordinator Admin API create/scale
* Coordinator heartbeat API
* Basic Auth admin/password access control
* Monitoring API
* member heartbeat record
* member scale-out/scale-in sequence
* idle member expiration and cleanup
* group epoch, assignment epoch, member epoch
* Coordinator Admin API consumer concurrency update
* server-side sticky partition assignment
* target assignment store
* current assignment reporting
* revoke before assign dependency handling
* resharding
* coordinator/member metrics
* coordinator Docker image path and local Compose deployment
* open source contribution, testing, security, release, and operations documentation

Excluded:

* Kafka protocol compatibility
* member application에 embedded된 coordinator mode
* Redis Stream message processing, handler execution, retry, DLQ
* pending recovery and idempotency marker implementation
* multiple concurrent migrations per stream prefix
* admin UI
* hot shard auto split
* Redis Cluster resharding automation
* single-processing guarantees
* producer global deduplication and cross-shard/cross-version event id deduplication

## Tradeoffs

### Coordinator-Managed Assignment

Benefits:

* 운영자가 target/current assignment를 한 곳에서 확인할 수 있다.
* revoke before assign 순서를 coordinator가 강제한다.
* member expiration과 fencing을 명확히 모델링할 수 있다.
* member client logic이 단순해진다.

Costs:

* coordinator state store가 운영상 중요해진다.
* coordinator bug는 group 전체 assignment에 영향을 줄 수 있다.

### Coordinator API Control Plane

Benefits:

* shard count, consumer concurrency, migration mutation 경로가 하나로 고정된다.
* member application YAML이 group metadata의 source of truth가 되지 않는다.
* 운영자가 API와 Redis metadata에서 group 상태를 일관되게 확인할 수 있다.

Costs:

* coordinator API 배포와 인증/인가를 별도로 운영해야 한다.
* coordinator API 장애 시 신규 mutation과 heartbeat reconciliation이 지연된다.
* coordinator API 장애 중에는 신규 heartbeat assignment 전달이 지연된다.

## Risks

* target assignment write와 member current assignment 처리 순서가 꼬이면 stale member가 잘못된 owner로 남을 수 있다. Coordinator는 heartbeat ownership report를 target/current assignment 기준으로 검증하고 unauthorized ownership report를 fencing한다.
* member lease TTL이 너무 짧으면 일시적 네트워크 지연에도 EXPIRED 처리가 발생할 수 있다.
* Redis outage 시 coordinator control plane, stream data plane, optional provisioning이 영향받는다.
* assignment state가 손상되면 수동 복구 runbook이 필요하다.
* shard count migration 중 old/new key ordering이 깨질 수 있다.
* shard scale-out/in 중 producer가 같은 event id를 old routing metadata와 new routing metadata로 각각 publish하면 duplicate message가 발생할 수 있다. 중복에 민감한 workload는 scale 전 producer quiescence가 필요하다.
* 여러 비즈니스 side effect를 Redis Stream ACK와 하나의 원자적 transaction으로 묶을 수 없으므로 단일 처리 보장은 프로젝트 보장으로 제공하지 않는다.
* public Docker image release에는 version tag, compatibility note, and smoke result가 함께 공개되어야 한다.

## Processing Guarantee Boundary

기본 처리 보장은 at-least-once이다.

Included scope:

* coordinator `memberEpoch` / `assignmentEpoch` 기반 stale owner fencing
* revoke-before-assign handoff
* pending recovery가 가능한 Redis Stream consumer group 사용
* duplicate-tolerant application 설계를 위한 shard/progress visibility

Excluded scope:

* single delivery
* single handler invocation
* single business side effects
* producer global deduplication
* shard scale 중 old/new routing metadata를 가로지르는 global event id deduplication

## Maintainability Improvements

* coordinator와 support module 간 compatibility는 `protocolVersion`으로 관리한다. 호환되지 않는 request/response 변경은 major version bump로 처리하고, optional field 추가만 같은 version에서 허용한다. 상세 정책은 [`11-versioning-compatibility.md`](11-versioning-compatibility.md)에서 관리한다.
* coordinator event loop에는 invariant checker를 둔다. target assignment 중복 owner, missing owner, stale member epoch을 감지하면 metric과 structured log를 남긴다.
* `EXPIRED` member와 migration metadata는 삭제하지 않고 장기간 유지한다. 정리가 필요하면 별도 운영 작업으로 다룬다.
* Admin API mutation은 audit log를 필수로 남긴다. 운영자가 누가 언제 group metadata를 바꿨는지 추적할 수 있어야 한다.
* state transition, sticky assignment, heartbeat assignment deduplication은 table-driven test와 golden JSON fixture로 관리한다.
* runbook은 최소 세 가지를 제공한다: coordinator API unavailable, migration drain 지연, assignment invariant violation. Current runbook lives at [`../operations-runbook.md`](../operations-runbook.md).

## Open Questions

* member UUID를 member runtime이 직접 생성하는 방식을 유지할지, coordinator가 registration ack와 함께 부여할지?
* shard 분배 가중치는 MVP에서 동일 가중치로 고정할지?
* Metadata update에 별도 audit/history stream을 둘지?
* KIP-848과 다른 잔여 gap은 [`08-kip848-implementation-coverage.md`](08-kip848-implementation-coverage.md)의 `Residual Gaps To Decide`에서 관리한다.

## Decision Needed

* MVP coordinator deployment: direct Coordinator API control plane
* MVP assignment: sticky partition 고정
* MVP member identity: member-runtime-generated UUID
* MVP state store: Redis metadata hash + JSON aggregate + storeRevision CAS
* MVP access control: Basic Auth `admin/password`
* MVP rebalance style: coordinator-driven incremental reconciliation
* MVP shard count source of truth: Coordinator Admin API로 생성/변경된 coordinator metadata
* MVP consumer `maxConcurrency` source of truth: Coordinator Admin API로 변경된 consumer concurrency policy
* MVP coordinator deployment artifact: Java 24 Docker image plus Spring Boot jar
