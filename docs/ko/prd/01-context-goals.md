# Context, Goals, Non-Goals

## Context

Redis Stream에는 Kafka broker coordinator가 없으므로, 동일한 개념을 Redis-backed coordinator로 구현해야 한다.

## Goals

* coordinator가 group metadata, target assignment, current assignment를 source of truth로 관리한다.
* group 식별자는 coordinator API의 `{streamPrefix, consumerGroup}` path/body에서 받는다.
* member join/leave, metadata change, Coordinator Admin API로 요청된 shard count change를 group epoch 증가로 모델링한다.
* topic partition에 해당하는 Redis Stream shard count는 Coordinator Admin API 요청으로만 생성/증감한다.
* Annotation listener의 `concurrency = "N"`은 N개의 독립 coordinator member를 만드는 member fan-out이다. 각 member는 별도 heartbeat와 assignment state를 가진다.
* Bean 기반 `runtimeMaxConcurrency`는 partition/shard 개수가 아니라 하나의 member 안에서 사용할 consumer worker 수이며, coordinator의 server-side consumer concurrency policy로 제한된다.
* target assignment는 coordinator가 계산하고 assignment epoch으로 versioning한다.
* member는 heartbeat/reconciliation loop로 target assignment에 수렴한다.
* coordinator는 `tick-interval`마다 heartbeat가 `member-lease-ttl`을 넘긴 member를 `EXPIRED`로 표시하고 shard를 재할당한다.
* revoke 완료 전 같은 shard를 다른 member에게 assign하지 않는다.
* 영향 없는 member는 rebalance 중에도 기존 shard를 계속 consume한다.
* shard count 변경은 next resharding으로 처리한다. at-least-once producer는 online 전환할 수 있지만, 중복에 민감한 workload는 scale 전 producer quiescence를 운영 제약으로 둔다.
* actual read authority는 coordinator가 내려준 assignment epoch과 member epoch으로 fence한다.
* 기본 처리 보장은 at-least-once이다. application은 중복 전달과 중복 처리 시도를 허용하도록 설계해야 한다.
* operator가 group epoch, assignment epoch, member epoch, target/current assignment, revoke progress를 볼 수 있게 한다.

## Non-Goals

* Kafka broker protocol을 그대로 재구현
* Redis Cluster resharding 자동 대응
* hot shard 자동 split
* shard 전체 serial ordering
* 단일 delivery, 단일 handler invocation, 단일 side effect 보장
* producer global deduplication 또는 전체 shard/version을 가로지르는 동일 `eventId` deduplication
* 여러 비즈니스 side effect와 Redis Stream ACK를 원자적으로 묶는 transaction protocol
* 기존 stream key를 직접 rewrite하는 in-place resharding
* member startup, local YAML, producer, consumer가 직접 shard count나 server-side consumer `maxConcurrency`를 변경하는 방식
* 기본 consumer mode의 retry와 DLQ 구현
* coordinator server HA/election protocol 설계

## Assumptions

* Redis는 stream data plane으로 사용한다.
* Coordinator metadata store는 Redis-backed 단일 metadata key이다.
* 각 member는 runtime 시작 시 pod IP context에서 `memberId`를 직접 생성한다.
* producer와 consumer는 coordinator가 Redis metadata key 기준으로 내려준 shard count을 source of truth로 사용한다.
* producer retry, network failure, consumer pending recovery, shard migration은 중복 message 또는 중복 처리 시도를 만들 수 있다.
* 동일 partition key는 같은 routing metadata 안에서만 같은 shard로 route된다. shard count가 바뀌면 다른 shard로 route될 수 있다.
* 한 `streamPrefix + consumerGroup`에는 동시에 하나의 active migration만 허용한다.
* `streamPrefix`와 `consumerGroup`은 coordinator API 호출의 group identifier이며, coordinator/member local YAML에 고정된 shard control 설정으로 두지 않는다.
* member startup은 shard count 변경을 시작하지 않는다. startup은 coordinator API heartbeat join과 metadata 조회만 수행한다.
* member heartbeat는 runtime이 감당 가능한 consumer worker 수를 보고할 수 있지만, 실제 허용되는 `maxConcurrency`는 coordinator metadata가 결정한다.
* Annotation listener concurrency와 bean 기반 worker capacity는 별도 개념이다.
