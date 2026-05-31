# Member Data-Plane Boundary

Redis Stream Coordinator는 message processor가 아니다. 이 문서는 coordinator와 member data-plane의 경계만 정의한다.

## Coordinator Owns

* group metadata
* member liveness
* target assignment
* current assignment report
* assigned/pending shard assignment
* member epoch fencing status
* stream version migration state
* assignment epoch과 member epoch 기반 stale owner fencing

## Member Data-Plane Owns

* Redis Stream `XREADGROUP` / `XACK`
* pending recovery
* handler execution
* retry / DLQ
* idempotency marker
* key ordering
* read batch, block timeout, worker thread tuning

## At-Least-Once Processing Model

이 프로젝트의 기본 처리 모델은 at-least-once이다. Redis Stream consumer group delivery는 `XREADGROUP`/`XACK` 기준으로 중복 전달될 수 있고, consumer handler도 장애, pending recovery, rebalance 이후 다시 실행될 수 있다.

단일 처리 보장은 제공하지 않는다. 실제 application handler는 하나의 message를 처리하면서 DB update, Redis write, HTTP call, external API call, local cache update 등 여러 비즈니스 side effect를 함께 수행할 수 있다. 이 프로젝트는 그런 side effect들과 Redis Stream ACK를 하나의 원자적 transaction으로 묶는 protocol을 제공하지 않는다.

Application 책임:

* handler는 중복 호출될 수 있다고 가정한다.
* business side effect는 domain-level idempotency, unique constraint, deduplication table, compensation 등으로 보호한다.
* Redis ACK는 application이 선택한 처리 정책에 따라 수행한다. ACK 전 crash는 재전달을 만들 수 있고, ACK 후 crash는 처리 유실을 만들 수 있으므로 application risk profile에 맞게 선택한다.
* coordinator가 내려준 `memberEpoch`, `assignmentEpoch`, shard ownership 정보는 stale owner fencing에 사용하지만, business side effect 중복 방지를 대신하지 않는다.

권장 처리 흐름:

```text
XREADGROUP
  -> memberEpoch / assignmentEpoch / shard ownership 확인
  -> business handler 실행
  -> application-level duplicate guard 또는 retry policy 적용
  -> XACKDEL 또는 XACK
```

보장 경계:

* Redis Stream delivery는 at-least-once이다.
* consumer handler execution은 at-least-once이다.
* producer publish는 retry와 migration 상황에서 duplicate를 만들 수 있다.
* 전체 shard/version을 가로지르는 동일 event id produce 중복 방지는 제공하지 않는다.
* 외부 side effect의 단일 반영은 제공하지 않는다.

## Contract

Coordinator heartbeat response의 `assignment.assignedShards`에서 기존 owned shard가 빠지면 member는 해당 shard의 신규 read를 중단하고 local in-flight가 0이 된 뒤 heartbeat의 `revokingShards.state=REVOKED`로 보고한다.

`OK` response의 `assignment.assignedShards`에 새 shard가 포함되면 member는 shard를 `ownedShards`에 반영하고, 실제 Redis Stream read/recovery는 member data-plane 정책에 따라 수행한다.

Metadata correction 중 `SYNC_METADATA`와 `REVOKE_PENDING`은 drain-only response이다. Member는 이미 소유한 shard 중 `assignment.assignedShards`에 남은 shard만 유지할 수 있고, 처음 등장한 shard는 이후 `OK` response를 받을 때까지 read를 시작하면 안 된다.

Coordinator가 `FENCED_MEMBER_EPOCH`을 반환하거나 member epoch mismatch가 발생하면 member는 read/ack를 중단하고 `memberEpoch=0` full heartbeat로 rejoin한다.

Coordinator는 member가 보고한 `ownedShards`/`revokingShards`를 server-side target assignment와 이전에 수락한 current assignment 기준으로 검증한다. 아직 pending인 shard, 다른 live member가 소유 중인 shard, 또는 더 이상 허용되지 않는 shard를 owned로 보고하면 stale ownership으로 보고 fencing한다. 이미 처리된 terminal `REVOKED` duplicate report는 fencing하지 않고 무시한다.
