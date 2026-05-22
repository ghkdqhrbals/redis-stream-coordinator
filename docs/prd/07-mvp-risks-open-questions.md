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
* stream version migration
* coordinator/member metrics

Excluded:

* Kafka protocol compatibility
* member application에 embedded된 coordinator mode
* Redis Stream message processing, handler execution, retry, DLQ
* pending recovery and idempotency marker implementation
* multiple concurrent migrations per stream prefix
* admin UI
* hot shard auto split
* Redis Cluster resharding automation
* exactly-once external side effects

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

* target assignment write와 member current assignment 처리 순서가 꼬이면 stale member가 잘못된 owner로 남을 수 있다.
* member lease TTL이 너무 짧으면 일시적 네트워크 지연에도 EXPIRED 처리가 발생할 수 있다.
* Redis outage 시 coordinator와 data plane이 함께 영향받는다.
* assignment state가 손상되면 수동 복구 runbook이 필요하다.
* shard count migration 중 old/new key ordering이 깨질 수 있다.

## Maintainability Improvements

* heartbeat schema는 `protocolVersion`으로 관리한다. 호환되지 않는 request/response 변경은 major version bump로 처리하고, optional field 추가만 같은 version에서 허용한다.
* coordinator event loop에는 invariant checker를 둔다. target assignment 중복 owner, missing owner, stale member epoch을 감지하면 metric과 structured log를 남긴다.
* `EXPIRED` member와 migration metadata는 삭제하지 않고 장기간 유지한다. 정리가 필요하면 별도 운영 작업으로 다룬다.
* Admin API mutation은 audit log를 필수로 남긴다. 운영자가 누가 언제 group metadata를 바꿨는지 추적할 수 있어야 한다.
* state transition, sticky assignment, heartbeat assignment deduplication은 table-driven test와 golden JSON fixture로 관리한다.
* runbook은 최소 세 가지를 제공한다: coordinator API unavailable, migration drain 지연, assignment invariant violation.

## Open Questions

* member UUID를 member runtime이 직접 생성하는 방식을 유지할지, coordinator가 registration ack와 함께 부여할지?
* shard 분배 가중치는 MVP에서 동일 가중치로 고정할지?
* Redis key update는 Lua CAS만 쓸지, Redis Streams event log를 둘지?
* KIP-848과 다른 잔여 gap은 [`08-kip848-implementation-coverage.md`](08-kip848-implementation-coverage.md)의 `Residual Gaps To Decide`에서 관리한다.

## Decision Needed

* MVP coordinator deployment: direct Coordinator API control plane
* MVP assignment: sticky partition 고정
* MVP member identity: member-runtime-generated UUID
* MVP state store: Redis HASH/JSON + Lua CAS
* MVP access control: Basic Auth `admin/password`
* MVP rebalance style: coordinator-driven incremental reconciliation
* MVP shard count source of truth: Coordinator Admin API로 생성/변경된 coordinator metadata
* MVP consumer `maxConcurrency` source of truth: Coordinator Admin API로 변경된 consumer concurrency policy
