# Terraform and GitOps Governance

## 목적

Coordinator admin API는 control-plane source of truth를 변경한다. Group 생성/삭제, shard count, consumer concurrency policy, resharding rollback이 여기에 해당한다. 운영 환경에서는 이런 mutation을 가능하면 Terraform 또는 GitOps workflow로 검토하고 적용해야 한다.

Terraform은 runtime audit log를 대체하지 않는다. Terraform은 desired state, plan output, 승인 이력, 누가 merge/apply했는지를 남긴다. Coordinator audit log는 실제 API에 도착한 요청, 실패한 요청, forbidden 요청, request id, caller identity, client address, request summary, request body fingerprint를 남긴다.

## 관리 경계

Terraform 또는 GitOps가 관리하기 좋은 것:

| Desired state | Coordinator API |
| --- | --- |
| Group 존재 여부 | `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` and `DELETE /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` |
| Initial shard count | create group request |
| Target shard count | `POST /coord/v1/streams/{streamPrefix}/scale` |
| Consumer concurrency policy | `PATCH /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/consumer-concurrency` |
| 운영 사유와 actor metadata | mutation request body plus `X-Request-Id` |

Terraform 또는 GitOps가 관리하면 안 되는 것:

| Runtime state | 이유 |
| --- | --- |
| Member heartbeat | 일시적인 liveness와 ownership reconciliation이다. |
| Current assignment report | member가 보고하는 관측값이지 desired state가 아니다. |
| Redis Stream offset, pending entry, message payload | data plane 책임이다. |
| Revoke/drain progress | live rebalance transition이다. |
| Producer routing cache | application runtime local cache이다. |

## 권장 Provider Contract

운영 mutation을 사용자가 local script로 직접 작성하게 하기보다, stable `/coord/v1` API 위에 Terraform provider를 제공하는 방향이 좋다.

권장 resource:

| Resource | 책임 |
| --- | --- |
| `redisstreamcoordinator_group` | `{streamPrefix, consumerGroup}` metadata record, initial shard count, default consumer concurrency policy를 관리한다. |
| `redisstreamcoordinator_group_shard_count` | 기존 group의 shard count 변경을 적용하고, coordinator가 새 producer routing metadata를 노출할 때까지 기다린다. |
| `redisstreamcoordinator_consumer_concurrency_policy` | `defaultMaxConcurrency`와 member-name override를 관리한다. |

Provider import id는 다음 형식을 권장한다.

```text
{streamPrefix}/{consumerGroup}
```

Provider mutation call은 다음 header를 포함해야 한다.

```http
X-Request-Id: terraform-${workspace}-${run_id}
Authorization: Basic <write-user>
```

Mutation request body에는 다음 값을 넣는다.

```json
{
  "requestedBy": "terraform:<workspace>",
  "reason": "terraform apply <run id>"
}
```

## 제공 Terraform Module

현재 repository에는 초기 Terraform module이 포함되어 있다.

```text
terraform/modules/stream-group
```

이 module은 전용 provider가 나오기 전에도 Terraform으로 shard count를 관리할 수 있도록 `terraform_data`와 작은 `curl` 실행 스크립트를 사용해 Coordinator Admin API를 호출한다.

관리 입력값:

| 입력값 | 의미 |
| --- | --- |
| `coordinator_base_url` | Coordinator API base URL |
| `coordinator_username` / `coordinator_password` | 선택적 Basic Auth 인증 정보. `WRITE` 권한 필요 |
| `stream_prefix` | Redis Stream shard prefix |
| `consumer_group` | Redis Stream consumer group |
| `shard_count` | 원하는 physical shard count. 값이 바뀌면 `/scale`을 호출 |
| `requested_by` / `reason` | coordinator audit log에 남길 metadata |

Apply 동작:

1. `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}`로 group이 없으면 생성한다.
2. 이미 존재하는 group의 `409`는 정상 조건으로 처리한다.
3. `GET /producer-routing`으로 현재 shard count를 읽는다.
4. 현재 `shardCount`와 Terraform의 `shard_count`가 다르면 `POST /coord/v1/streams/{streamPrefix}/scale`을 호출한다.
5. Producer routing metadata가 원하는 shard count를 노출할 때까지 poll한다.

Destroy 동작은 의도적으로 no-op이다. `terraform destroy`가 실수로 coordinator metadata나 active assignment를 삭제하면 안 된다. Group 삭제는 audit log와 safety validation이 있는 Coordinator Admin API를 명시적으로 호출하는 작업으로 유지한다.

예시:

```hcl
module "create_order_group" {
  source = "../../modules/stream-group"

  coordinator_base_url = "https://api.example.com"
  coordinator_username = var.coordinator_username
  coordinator_password = var.coordinator_password

  stream_prefix  = "create-order"
  consumer_group = "demo-workers"
  shard_count    = 20

  requested_by = "terraform:${terraform.workspace}"
  reason       = "scale create-order stream group"
}
```

## Drift Detection

Terraform read operation은 coordinator source-of-truth API를 호출해야 한다.

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing
```

Provider가 managed desired state로 다룰 field:

* group existence,
* `shardCount`,
* `consumerConcurrencyPolicy.defaultMaxConcurrency`,
* `consumerConcurrencyPolicy.memberOverrides`.

Provider가 computed runtime state로 다룰 field:

* `groupEpoch`,
* `assignmentEpoch`,
* `metadataVersion`,
* `members`,
* `targetAssignments`,
* `currentAssignments`,
* `migrations`,
* `revokeProgress`.

## Audit Evidence

Terraform이 caller여도 모든 admin mutation은 coordinator audit event를 남겨야 한다.

Audit event에 포함해야 할 값:

* `action`,
* `outcome`,
* HTTP status,
* authenticated principal,
* `requestedBy`,
* `reason`,
* `requestId`,
* client address,
* user agent,
* roles,
* route and path,
* duration,
* stream prefix,
* consumer group,
* resharding id when present,
* request summary,
* SHA-256 request body fingerprint.

Redis-backed audit이면 event는 다음 key에 저장한다.

```text
redis-stream:coord:{streamPrefix:consumerGroup}:admin:audit
```

Log-backed audit이면 structured application log로 남긴다.

## Failure Handling

Terraform apply는 accepted resharding이 즉시 완료됐다고 가정하면 안 된다. Scale API는 `202 Accepted`를 반환하고, consumer는 heartbeat reconciliation을 통해 점진적으로 수렴한다.

권장 flow:

1. Desired shard count를 apply한다.
2. Producer routing metadata의 `shardCount`가 target과 같아질 때까지 poll한다.
3. Monitoring API로 assignment와 revoke progress가 정상인지 확인한다.
4. Coordinator가 incompatible active resharding이나 unsafe delete condition을 보고하면 Terraform run을 실패시킨다.

Duplicate-sensitive workload는 shard count 변경 전 producer를 여전히 quiesce해야 한다. Terraform은 admin mutation을 orchestrate할 수 있지만, application이 별도 신호를 제공하지 않으면 모든 publish retry가 drain됐는지 증명할 수 없다.
