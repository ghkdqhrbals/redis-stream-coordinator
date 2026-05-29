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

1. Producer routing metadata의 `activeWriteVersion`이 기대값인지 확인한다.
2. Live consumer가 target assignment에 수렴했는지 확인한다.
3. Old-version shard가 `currentAssignments`나 `revokeProgress`에 남아 있는지 확인한다.
4. Active resharding이 위험하면 old version이 deprecated되기 전에 rollback API를 사용한다.

## Shard Scale Procedure

Duplicate를 허용하는 at-least-once workload는 coordinator scale API로 online scale-out/in을 수행할 수 있다.

Duplicate-sensitive workload는 다음 순서를 지킨다.

1. 대상 `streamPrefix`와 `consumerGroup`의 producer를 멈춘다.
2. In-flight `XADD`와 publish retry window가 drain될 때까지 기다린다.
3. Coordinator scale API를 호출한다.
4. Producer routing metadata가 새 `activeWriteVersion`을 노출할 때까지 기다린다.
5. Producer routing cache를 refresh한다.
6. Producer를 재개한다.

이 절차가 필요한 이유는 scale 중 produce retry가 old/new stream version에 같은 event id를 각각 publish할 수 있기 때문이다.

## Upgrade Procedure

1. Release note와 compatibility matrix를 읽는다.
2. Coordinator가 기존 consumer의 heartbeat protocol range를 지원하는지 확인한다.
3. Redis metadata key를 backup한다.
4. 새 coordinator version을 배포한다.
5. `/coord/v1/monitoring/health`를 확인한다.
6. Consumer application을 점진적으로 rolling한다.
7. Member expiry, rebalance duration, revoke pending, invariant metrics를 관찰한다.

## Redis Metadata Backup

Coordinator keys use one Redis Cluster hash tag per group:

```text
redis-stream:coord:{streamPrefix:consumerGroup}:*
```

Schema-changing upgrade 전에는 configured `coordinator.store.key-prefix` 아래 key를 backup한다.

