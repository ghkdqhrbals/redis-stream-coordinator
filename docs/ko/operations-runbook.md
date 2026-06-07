# 운영 Runbook

이 runbook은 coordinator control plane 운영에 초점을 둔다. Application message handling, retry, DLQ, idempotency는 consumer application 책임이다.

## Health Check

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/health
```

| Field | Meaning |
| --- | --- |
| `status=UP` | Coordinator HTTP API와 필요한 dependency가 정상이다. |
| `status=DEGRADED` | Coordinator는 떠 있지만 Redis health가 실패했다. |
| `redis=NOT_CONFIGURED` | 현재 설정에서 Redis가 필수 dependency가 아니다. |

## 자주 보는 운영 API

Group 목록:

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/groups
```

Assignment 조회:

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/streams/orders/groups/orders-consumer/assignments
```

Resharding 조회:

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/streams/orders/groups/orders-consumer/migrations
```

## Terraform/GitOps Admin Mutation

운영 환경의 admin mutation은 가능하면 Terraform 또는 GitOps workflow로 적용한다.

권장 패턴:

1. Pull request에서 desired change를 review한다.
2. Coordinator read API 기준으로 plan을 확인한다.
3. 전용 `WRITE` principal로 apply한다.
4. `X-Request-Id`, request body의 `requestedBy`, `reason`을 반드시 넣는다.
5. Apply 후 coordinator audit log와 monitoring API를 확인한다.

Terraform은 group existence, shard count 같은 desired state를 관리한다. Consumer runtime concurrency, heartbeat, current assignment, revoke progress, offset, pending entry, message payload 같은 runtime state는 Terraform 관리 대상이 아니다.

Terraform이 caller여도 coordinator audit log는 필수이다. 실제 API request, outcome, status, principal, roles, request id, request body fingerprint, client address, duration, stream prefix, consumer group, operation reason을 coordinator가 남긴다.

## Alert 후보

* coordinator health `DEGRADED`
* Redis connection failure
* `redis_stream_coord_member_expired_total` 급증
* `redis_stream_coord_member_heartbeat_age_seconds`가 member lease TTL에 근접
* `redis_stream_coord_revoke_pending`이 오래 유지됨
* `redis_stream_coord_invariant_violation_total` 증가
* `redis_stream_coord_state_conflict_total` 급증
* `redis_stream_coord_consumer_shard_pending`이 특정 member/shard에서 높게 유지됨
* active resharding age가 예상 drain window를 초과
* admin automation에서 `429` 반복 발생

## Rebalance Triage

1. Members monitoring API로 member liveness를 확인한다.
2. Assignment와 `revokeProgress`를 확인한다.
3. 특정 member가 revoke 중 멈춰 있으면 해당 application instance의 slow handler, blocked shutdown, in-flight work를 확인한다.
4. Rebalance timeout이 지나면 coordinator는 해당 member를 fence하고 reassignment를 진행한다.
5. `FENCED_MEMBER_EPOCH`을 받은 consumer는 local ownership을 중단하고 `memberEpoch=0`으로 rejoin해야 한다.

## Resharding Triage

1. Producer routing metadata의 `shardCount`이 기대값인지 확인한다.
2. Live consumer가 target assignment에 수렴했는지 확인한다.
3. Old-version shard가 `currentAssignments`나 `revokeProgress`에 남아 있는지 확인한다.
4. Active resharding이 위험하면 rollback 허용 구간 안에서 rollback API를 사용한다.

## Shard Scale Procedure

Duplicate를 허용하는 at-least-once workload는 coordinator scale API로 online scale-out/in을 수행할 수 있다.

Duplicate-sensitive workload는 다음 순서를 지킨다.

1. 대상 `streamPrefix`와 `consumerGroup`의 producer를 멈춘다.
2. In-flight `XADD`와 publish retry window가 drain될 때까지 기다린다.
3. Coordinator scale API를 호출한다.
4. Producer routing metadata가 새 `shardCount`을 노출할 때까지 기다린다.
5. Producer routing cache를 refresh한다.
6. Producer를 재개한다.

이 절차가 필요한 이유는 scale 중 produce retry가 old/new shard count에 같은 event id를 각각 publish할 수 있기 때문이다.

## Upgrade Procedure

1. Release note와 compatibility matrix를 읽는다.
2. Coordinator가 기존 consumer의 coordination version range를 지원하는지 확인한다.
3. Coordinator Redis metadata key를 backup한다.
4. 새 coordinator version을 배포한다.
5. `/coord/v1/monitoring/health`를 확인한다.
6. Consumer application을 점진적으로 rolling한다.
7. Member expiry, rebalance duration, revoke pending, invariant metrics를 관찰한다.

## Redis Metadata Backup

Coordinator metadata는 group별 Redis hash key 하나에 저장한다.

```text
redis-stream:coord:{streamPrefix:consumerGroup}:metadata
```

Schema-changing upgrade와 수동 repair 전에는 coordinator metadata key를 backup한다.

## Metadata Durability

Redis metadata key가 해당 group의 coordinator source of truth이다. Consumer가 Redis에 저장된 값보다 높은 `metadataVersion`을 보고하더라도 coordinator는 그 값을 신뢰하지 않는다. 대신 현재 Redis metadata로 맞추도록 retry-safe `SYNC_METADATA` 응답을 내려준다. Consumer가 현재 version을 보고한 뒤에도 revoke/drain이 handoff를 막고 있으면 `REVOKE_PENDING`을 받고, 신규 shard read가 가능한 시점에만 `OK`를 받는다.

Production 권장사항:

1. Deployment에 맞는 Redis persistence와 backup을 사용한다.
2. Coordinator metadata key를 전용 `coordinator.store.key-prefix` 아래에 둔다.
3. Application runtime user가 coordinator metadata key를 delete할 수 없게 한다.
4. Schema-changing upgrade나 수동 maintenance 전에는 coordinator metadata key를 backup한다.
5. 오래된 Redis backup restore는 일반 retry가 아니라 disaster recovery로 취급한다.

Metadata rollback signal은 다음과 같다.

1. Consumer heartbeat가 coordinator에 저장된 값보다 높은 `metadataVersion`, `assignmentEpoch`, `memberEpoch`를 이미 관측했다고 보고한다.
2. Producer routing cache가 coordinator response보다 높은 `metadataVersion`을 이미 관측했다.
3. Coordinator metric에서 metadata version regression 또는 store revision regression이 발생했다.
4. Rebalance state가 새 assignment epoch 없이 released에서 revoking으로 돌아가는 등 이전 상태로 되돌아간다.

Redis metadata에는 이 한계가 남는다. Redis가 rollback됐지만 더 높은 version을 보고할 살아있는 consumer 또는 producer가 없다면 coordinator는 rollback을 감지하지 못할 수 있다.

Source-of-truth metadata key가 사라졌거나 Redis가 오래된 backup으로 restore됐다면:

1. 해당 group의 admin mutation을 중단한다.
2. key가 delete, corruption, 오래된 backup restore 중 무엇으로 사라지거나 rollback됐는지 확인한다.
3. 가능하면 backup에서 metadata를 restore한다.
4. backup이 없거나 client가 관측한 최고 version보다 오래된 backup뿐이라면 기대 shard count로 group을 명시적으로 재생성하고, consumer/producer를 새 group lifecycle로 취급한다.
5. Consumer heartbeat, producer routing cache, stale local state로 group metadata를 자동 재구성하지 않는다.
6. Client를 낮은 metadata version으로 강제 downgrade하지 않는다. Repair 또는 recreation이 끝날 때까지 fail closed한다.
7. Rollback된 version 숫자만 증가시켜 repair하지 않는다. 유실된 transition에는 drain, release, shard scale, routing decision이 포함될 수 있고 이를 안전하게 추론할 수 없다.
