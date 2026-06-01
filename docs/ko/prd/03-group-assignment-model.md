# Group Metadata and Assignment Model

## Group Metadata

```json
{
  "streamPrefix": "summary",
  "consumerGroup": "summary-group",
  "groupEpoch": 12,
  "metadataVersion": 8,
  "assignmentEpoch": 11,
  "state": "STABLE",
  "shardCount": 2,
  "readableShardSet": [1, 2],
  "updatedAt": "2026-05-15T10:00:00Z"
}
```

`groupEpoch`은 group metadata가 바뀔 때 증가한다.
Stream version 값은 정수이며 prefix 문자열을 붙이지 않는다.

Trigger:

* member join
* member lease TTL 만료
* member metadata 변경
* managed shard set 변경
* Coordinator Admin API로 요청된 shard count migration 시작/완료
* coordinator가 member를 fenced 처리

## Group State

`state`는 group이 target assignment에 어느 정도 수렴했는지를 나타낸다.

| State | Meaning |
| --- | --- |
| `EMPTY` | 살아 있는 member가 없고 readable shard owner도 없다. |
| `ASSIGNING` | `groupEpoch > assignmentEpoch`라서 coordinator가 새 target assignment를 계산해야 한다. |
| `RECONCILING` | target assignment는 계산됐지만 일부 member가 revoke/assign을 아직 완료하지 않았다. |
| `STABLE` | 모든 active member의 current assignment가 target assignment와 같은 epoch으로 수렴했다. |



State transition:

```text
EMPTY
  -> ASSIGNING      first live member joins

STABLE
  -> ASSIGNING      member join, member leave, idle removal, or readable metadata change
  -> EMPTY          last member leaves and no readable shard can be owned

ASSIGNING
  -> RECONCILING    target assignment persisted for current groupEpoch

RECONCILING
  -> STABLE         all active members converge
  -> ASSIGNING      another metadata change happens before convergence

```

## Epoch Model

`epoch`은 시간이 아니라 stale actor를 막기 위한 세대 번호이다. coordinator는 epoch을 단조 증가시키고, member는 자신이 들고 있는 epoch이 최신인지 검증한 뒤 assignment를 적용한다.

| Epoch | Owner | Increments When | Used For |
| --- | --- | --- | --- |
| `groupEpoch` | coordinator | member join/leave, idle removal, shard migration, readable metadata 변경 | 새 target assignment가 필요한지 판단 |
| `assignmentEpoch` | coordinator | target assignment를 새 group metadata 기준으로 저장할 때 | target assignment가 어떤 group epoch 기준인지 표시 |
| `memberEpoch` | coordinator | member가 새 assignment를 적용해야 하거나 fencing될 때 | stale member heartbeat/read/ack 차단 |

Invariant:

* `assignmentEpoch`은 target assignment를 계산한 `groupEpoch`과 같아야 한다.
* active member의 `memberEpoch`은 자신에게 내려간 target assignment epoch으로 수렴해야 한다.
* member data-plane은 `memberEpoch`과 `assignmentEpoch`이 유효한 assignment에서만 read/ack한다.
* `STABLE` group에서는 active member의 current assignment 합집합이 target assignment와 같아야 한다.

## Member Metadata

```json
{
  "memberId": "018f8d27-8d71-7a7c-a7e7-b3f77c613d7f",
  "memberName": "summary-consumer-a",
  "state": "ACTIVE",
  "memberEpoch": 11,
  "metadataVersion": 8,
  "assignedMaxConcurrency": 12,
  "runtimeMaxConcurrency": 12,
  "activeConsumerWorkers": 8,
  "currentAssignment": [
    {"shardIndex": 1},
    {"shardIndex": 3}
  ],
  "revoking": [],
  "lastHeartbeatAt": "2026-05-15T10:00:00Z",
  "memberLeaseExpiresAt": "2026-05-15T10:00:15Z"
}
```

Member metadata는 member lifecycle의 source of truth이다.

* `lastHeartbeatAt`: coordinator가 마지막 heartbeat를 정상 반영한 시각이다.
* `memberLeaseExpiresAt`: 이 시각 이후에는 `EXPIRED`로 fencing하고 재할당을 시작한다.
* `assignedMaxConcurrency`: Coordinator Admin API가 저장한 server-side consumer concurrency policy 기준으로 허용된 consumer worker 수이다.
* `runtimeMaxConcurrency`: member process가 heartbeat로 보고한 local consumer worker 상한이다.
* `activeConsumerWorkers`: member가 현재 실행 중인 consumer worker 수이다.
* `maxConcurrency`는 partition/shard 개수가 아니며, target assignment의 shard 수 상한으로 쓰지 않는다.

## Logical Member와 Worker Capacity

설계에서는 논리 coordinator member와 member 내부 worker capacity를 분리한다.

Annotation 기반 consumer는 Kafka와 유사한 listener concurrency 모델을 사용한다. `@StreamListener(concurrency = "4")`는 같은 애플리케이션 프로세스 안에 4개의 논리 coordinator member를 만든다. Starter는 생성된 base member ID에 `-m0`, `-m1`, `-m2`, `-m3` 같은 suffix를 붙여 member ID를 만든다. 각 논리 member는 별도 heartbeat loop, Redis consumer name, assignment state, member epoch, metadata version, shard ownership을 가진다. Annotation listener에는 하나의 member로 합치는 toggle이 없다.

Bean 기반 consumer는 `CoordinatorConsumerProperties.consumer(...)`로 하나의 coordinator member를 명시적으로 등록할 수 있다. 이 경로에서 `runtimeMaxConcurrency`는 그 member 하나의 local worker capacity이다. Handler execution 또는 poll worker를 몇 개까지 동시에 실행할 수 있는지를 의미하며, 추가 coordinator member를 만들지는 않는다.

이 분리는 ownership 규칙을 단순하게 유지한다. Coordinator는 shard를 live member에 assign하고, 각 member는 자신이 소유한 shard들을 local worker capacity 안에서 어떻게 순회할지 결정한다.

## Target Assignment

Target assignment는 coordinator가 계산한 desired state이다.

```json
{
  "streamPrefix": "summary",
  "consumerGroup": "summary-group",
  "assignmentEpoch": 12,
  "groupEpoch": 12,
  "members": {
    "member-a": [
      {"shardIndex": 0}
    ],
    "member-b": [
      {"shardIndex": 1}
    ]
  }
}
```

Invariant:

* readable shard는 정확히 하나의 target owner를 가진다.
* unknown member에게 shard를 assign하지 않는다.
* `DRAINING` shard도 readable이면 assignment 대상이다.
* revoke 완료 전 shard는 새 member의 effective assignment에 포함하지 않는다.

## Current Assignment

Current assignment는 member가 실제로 적용 중이라고 보고한 state이다.

```json
{
  "memberId": "member-a",
  "memberEpoch": 11,
  "owned": [
    {"shardIndex": 0}
  ],
  "revoking": [
    {"shardIndex": 4}
  ],
  "ackedRevocations": [
    {"shardIndex": 4}
  ]
}
```

Coordinator는 current assignment를 보고 revoke ack가 들어온 shard만 새 owner에게 assign한다. 단, heartbeat report는 그대로 신뢰하지 않는다. Coordinator가 수락하는 ownership report는 다음 범위로 제한된다.

* member에게 이미 coordinator가 수락했던 `currentAssignment` 또는 `revoking` shard
* target assignment에 포함되어 있고 다른 live member의 current/revoking state에 의해 block되지 않은 shard
* terminal `REVOKED` duplicate report는 이미 release된 shard여도 무해하게 무시

member가 아직 `pendingShards` 상태인 shard나 다른 active member가 소유한 shard를 `ownedShards`로 보고하면 coordinator는 해당 member를 fenced 처리하고 `FENCED_MEMBER_EPOCH`을 반환한다.

## Member Removal State

Member 제거는 두 경로로 진행된다.

* graceful scale-in: member가 `memberEpoch=-1` heartbeat를 보내면 coordinator가 해당 member의 assignment를 비우고 revoke ack를 기다린다.
* idle removal: heartbeat가 `member-lease-ttl`을 넘기면 coordinator가 member를 `EXPIRED`로 fencing하고 재할당한다.

`EXPIRED` member metadata는 즉시 삭제하지 않는다. 운영 디버깅과 group 이력 추적을 위해 장기간 유지한다.

## Sticky Partition Assignment

이 설계는 sticky partition assignment를 고정 전제로 둔다. member가 선택하거나 heartbeat로 보고할 값이 아니다.

목표:

* shard를 live member 사이에 균등 분배한다.
* 기존 owner를 최대한 유지한다.
* revoke가 필요한 shard만 이동한다.
* `ACTIVE`와 `DRAINING` readable shard을 모두 assignment 대상으로 포함한다.

Sticky partition assignment는 현재 target assignment를 입력으로 사용해 movement cost를 최소화한다. scale-out에서는 필요한 일부 shard만 신규 member로 이동하고, scale-in에서는 사라지는 member의 shard만 살아 있는 member에게 재분배한다.

### Sticky Assignment Algorithm

Input:

* readable shard set: 현재 shard count의 모든 shard
* live member set: `ACTIVE` 또는 `STARTING` member 중 `member-lease-ttl`이 유효한 member
* excluded member set: `LEAVING`, `EXPIRED`, `FENCED` member
* previous target assignment
* optional member weight: consumer worker 수를 load balancing 가중치로만 사용할 수 있다.

Steps:

1. previous owner가 live member이면 기존 shard owner를 우선 유지한다.
2. excluded member가 가진 shard를 unassigned set으로 옮긴다.
3. unassigned shard를 `{shardIndex}` 기준으로 deterministic sort한다.
4. live member를 현재 shard load, optional member weight, `memberId` 순으로 정렬한다.
5. 각 unassigned shard를 가장 덜 찬 member에게 배정한다.
6. previous owner와 target owner가 다르면 coordinator는 previous owner의 `assignment.assignedShards`에서 해당 shard를 제외한다.
7. coordinator는 revoke ack 또는 expired member fencing을 확인한 뒤 target owner의 `assignment.assignedShards`에 해당 shard를 포함한다.

`maxConcurrency` 처리:

* Annotation listener의 `concurrency`는 논리 coordinator member 수이다. 하나의 member 내부 worker 수가 아니다.
* Bean 기반 `runtimeMaxConcurrency`는 member 내부 consumer worker 수이다.
* `runtimeMaxConcurrency`는 topic partition 수나 Redis Stream shard 수를 의미하지 않는다.
* Redis Stream shard는 한 시점에 정확히 하나의 live member만 소유한다.
* 하나의 member는 여러 shard를 소유할 수 있고, 소유 shard를 `assignedMaxConcurrency` 이하의 consumer worker가 multiplexing해서 읽는다.
* coordinator는 `maxConcurrency`를 처리량 가중치로 참고할 수 있지만, shard count 변경과 동일하게 취급하지 않는다.
* built-in polling adapter는 여러 worker가 있어도 같은 shard에 대해 동시에 Redis read를 수행하지 않는다.

Annotation listener에서 `concurrency = 4`는 독립적으로 assign되는 coordinator member 네 개를 뜻한다. Bean 기반 integration에서 `runtimeMaxConcurrency = 4`는 하나의 member가 최대 네 개의 handler execution을 병렬로 실행할 수 있다는 뜻이다. 어떤 경우에도 member가 shard 네 개까지만 소유할 수 있다는 뜻은 아니다.

하나의 member가 local worker 수보다 많은 shard를 소유하면 built-in polling adapter는 owned shard를 round-robin으로 순회한다. 뒤쪽 shard index도 계속 poll/processing되어야 하며, assignment 불균형이 영구적인 shard starvation으로 이어지면 안 된다.
