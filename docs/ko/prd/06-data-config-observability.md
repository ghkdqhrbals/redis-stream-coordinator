# Coordinator Data, Configuration, and Observability

## Scope

Redis Stream Coordinator는 KIP-848 스타일의 rebalance control plane이다. 이 서버는 group metadata, member liveness, target/current assignment, migration state를 관리한다. Coordinator metadata는 group별 Redis metadata hash key 하나에 저장한다.

Coordinator config에 두지 않는 것:

* `streamPrefix`, `consumerGroup`: API path/body의 group identifier이다.
* consumer handler thread, Redis Stream read batch, block timeout, ack mode.
* pending recovery, idempotency marker lifecycle.
* producer routing cache나 member runtime local tuning.

## Redis Metadata Store

Redis metadata store는 `{streamPrefix, consumerGroup}`당 하나의 hash key를 둔다.

```text
redis-stream:coord:{streamPrefix:consumerGroup}:metadata
```

| Field | 역할 |
| --- | --- |
| `aggregate` | Canonical `GroupMetadata` JSON aggregate |
| `revision` | `storeRevision` compare-and-set guard |
| `schemaVersion` | JSON aggregate schema version |
| `layoutVersion` | Redis metadata layout version |
| `updatedAt` | Last metadata write timestamp |

`aggregate`가 source of truth이다. `revision`, `schemaVersion`, `layoutVersion`, `updatedAt`은 compare-and-set, compatibility check, operational query를 위한 metadata field이며 Lua script로 같은 hash에 원자적으로 갱신한다.

Data-plane key는 coordinator 문서의 config에 넣지 않는다. 예를 들어 processing marker, stream read cursor, handler retry state는 member/consumer 구현 소관이다.

## Minimal Configuration

```yaml
coordinator:
  # Coordinator HTTP API 설정이다. group 식별자는 이 API path/body로 들어오며 YAML에 고정하지 않는다.
  api:
    # Coordinator API base path이다. create/scale/heartbeat/rollback endpoint가 이 path 아래에 열린다.
    base-path: /coord/v1
    # Admin API와 monitoring API 접근 계정이다. MVP는 Basic Auth로 시작한다.
    admin-username: admin
    # 운영 환경에서는 환경변수/secret으로 주입한다. 기본 local password는 password이다.
    admin-password: ${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD:password}
    # 선택 ACL이다. 비어 있으면 admin-username/admin-password가 READ/WRITE/MEMBER 전체 권한을 가진다.
    users:
      - username: admin
        password: ${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD:password}
        roles: [WRITE]
      - username: grafana
        password: ${REDIS_STREAM_COORDINATOR_MONITOR_PASSWORD:}
        roles: [READ]
      - username: member
        password: ${REDIS_STREAM_COORDINATOR_MEMBER_PASSWORD:}
        roles: [MEMBER]
    # true이면 member heartbeat API에도 MEMBER role 인증을 요구한다.
    authenticate-member-api: false
    # Admin mutation API에 per-caller/group fixed-window rate limit을 적용한다.
    rate-limit:
      enabled: false
      admin-mutations-per-minute: 60

  audit:
    # log 또는 redis. redis는 group-scoped admin audit list에 이벤트를 저장한다.
    sink: log
    redis-max-entries: 1000

  store:
    # local 개발은 memory, Redis 기반 metadata는 redis, DB 기반 metadata는 jdbc를 사용한다.
    type: redis
    key-prefix: redis-stream:coord

  # Redis Stream data plane과 optional stream provisioning에 사용할 Redis 접속 정보이다.
  redis:
    # Redis host이다.
    host: localhost
    # Redis port이다.
    port: 6379
    # Redis database index이다.
    database: 0
    # Redis ACL username이다. ACL을 쓰지 않으면 비워둘 수 있다.
    username:
    # Redis password이다. 운영 환경에서는 환경변수/secret으로 주입한다.
    password: ${REDIS_PASSWORD:}
    # Redis TLS 사용 여부이다.
    ssl: false

  # KIP-848 스타일 rebalance control plane의 동작 주기와 정책 경계이다.
  # member에게 HeartbeatResponse.heartbeatIntervalMs로 내려줄 권장 heartbeat 주기이다.
  heartbeat-interval: 3s
  # member heartbeat가 이 시간보다 오래 오지 않으면 EXPIRED/FENCED로 보고 target assignment를 다시 계산한다.
  member-lease-ttl: 15s
  # revoke/drain 완료를 기다리는 coordinator-owned 최대 시간이다. HeartbeatResponse.rebalanceTimeoutMs로 내려준다.
  rebalance-timeout: 60s
  loop:
    # Coordinator event loop 활성화 여부이다.
    enabled: true
    # heartbeat timeout, rebalance timeout, migration drain state를 이 주기마다 평가한다.
    tick-interval: 1s
  coordination:
    state-mutex:
      # Redis metadata store 접근을 직렬화하는 mutex 설정이다.
      enabled: true
      # 요청 처리 중 coordinator가 죽으면 이 시간이 지난 뒤 다른 instance가 critical section을 이어받는다.
      ttl-ms: 30000
      # mutex가 바쁠 때 heartbeat/admin/read-refresh 요청이 기다릴 최대 시간이다.
      acquire-timeout-ms: 5000
      # mutex 획득 재시도 간격이다.
      retry-interval-ms: 100

  # Admin API 요청에서 값이 생략됐을 때 적용되는 기본값이다.
  defaults:
    # create group 요청에서 initialShardCount가 없을 때 사용할 기본 shard count이다.
    initial-shard-count: 1
    # create/update 요청에서 maxConcurrency가 없을 때 member별로 내려줄 기본 consumer worker 수이다.
    consumer-max-concurrency: 1
```

Shard count와 consumer `maxConcurrency`의 실제 값은 Coordinator Admin API로 생성/변경된 group metadata에 저장한다. YAML의 `defaults`는 요청값이 생략됐을 때만 쓰이며, stream/group별 개별 설정은 Admin API로 저장한다. Kafka coordinator처럼 coordinator server config에는 shard count나 consumer concurrency min/max를 두지 않는다.

## Coordinator State Serialization

Redis-backed coordinator는 Redis mutex와 `storeRevision` compare-and-set을 state serialization boundary로 사용한다.

* create, heartbeat, scale, rollback, consumer concurrency update, monitoring read-time operational refresh, scheduled tick은 Redis mutex 안에서 group metadata hash를 읽고 갱신한다.
* critical section 순서는 `acquire Redis mutex -> read latest metadata hash -> validate/process/reconcile -> save with storeRevision CAS -> release mutex`이다.
* 여러 coordinator pod가 같은 Redis store를 보더라도 같은 group update는 mutex 또는 `storeRevision` CAS로 직렬화된다.
* event loop tick은 다른 instance가 먼저 같은 group을 update하면 reload하거나 skip하고 다음 tick에서 이어간다.
* memory store는 개발용이며 process-local state이므로 여러 coordinator replicas에 사용하지 않는다.

이 구조의 목적은 사용자가 `replicas=1`, `Recreate`, blue/green passive mode 같은 배포 세부사항을 직접 맞추지 않아도 안전하게 운영을 시작할 수 있게 하는 것이다.

## Metadata Store Options

Coordinator metadata store는 세 가지를 지원한다.

| Store | 용도 | 일관성 경계 |
| --- | --- | --- |
| `memory` | local 개발과 unit test | process-local map |
| `redis` | Redis만으로 운영하는 배포 | group metadata hash 1개 + Redis mutex + `storeRevision` CAS |
| `jdbc` | metadata를 DB에 저장해야 하는 배포 | `{streamPrefix, consumerGroup}` row 1개 + JSON metadata + `storeRevision` CAS |

JDBC store도 Redis store와 동일한 aggregate metadata JSON을 저장한다. primary key는 `{streamPrefix, consumerGroup}`이며 모든 update/delete는 이전 `storeRevision` 조건으로 보호한다.

## Access Control

MVP는 Basic Auth를 사용한다. `api.users`가 비어 있으면 `admin-username` / `admin-password`가 `READ`, `WRITE`, `MEMBER` 전체 권한을 가진다.

`api.users`가 설정되면 각 user의 role로 ACL을 평가한다.

* `READ`: monitoring, Grafana datasource, health, compatibility, message inspection API.
* `WRITE`: `READ` 전체와 create/delete/scale/rollback/producer routing metadata 같은 coordinator control-plane API.
* `MEMBER`: member heartbeat API. `authenticate-member-api=true`일 때만 요구한다.

기존 `ADMIN`, `MONITOR` role 이름은 하위 호환 alias로 허용한다. `ADMIN`은 legacy full coordinator permission, `MONITOR`는 `READ`로 처리한다.

인증 실패는 `401 Unauthorized`를 반환한다. 권한이 없는 mutation 요청은 `403 Forbidden`을 반환한다.

## API Rate Limiting

`coordinator.api.rate-limit.enabled=true`이면 create, scale, consumer concurrency update, rollback 같은 admin mutation API에 fixed-window rate limit을 적용한다. Key는 authenticated principal과 `{streamPrefix, consumerGroup}` 조합이다.

Rate limit 초과 시 coordinator는 `429 Too Many Requests`와 `Retry-After` header를 반환한다. Monitoring read API와 member heartbeat API는 이 limit에 포함하지 않는다. 현재 구현은 coordinator instance local limiter이므로, 여러 coordinator instance를 운영할 경우 global rate limit은 외부 gateway나 load balancer에서 보강한다.

## Monitoring API

Monitoring API는 coordinator state를 조회만 한다. 상태 변경은 Admin API로만 수행한다.
Health response는 active configuration 기준으로 dependency를 판단한다. Redis-backed store, Redis audit, stream provisioning 중 하나가 활성화된 경우에만 Redis health를 필수 dependency로 검사한다.

```http
GET /coord/v1/monitoring/health
GET /coord/v1/monitoring/groups
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/consumption
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations
```

Monitoring response에는 다음 요약만 포함한다.

* group state, `groupEpoch`, `assignmentEpoch`
* shard count
* active/expired member count
* target/current assignment summary
* revoke progress
* consumer-reported shard consumption progress, including stream key, shard, last delivered id, last acked id, pending count, and update time
* 관측 기반 초당 produce/consume 수
* active migration progress

## Metrics

Coordinator metric is the public observability surface. Consumer and producer modules may keep local diagnostics internally, but open-source users should rely on Coordinator metrics and monitoring APIs for group-level operation.

* `redis_stream_coord_up`
* `redis_stream_coord_group_epoch`
* `redis_stream_coord_assignment_epoch`
* `redis_stream_coord_members`
* `redis_stream_coord_member_active`
* `redis_stream_coord_member_heartbeat_age_seconds`
* `redis_stream_coord_member_lease_remaining_seconds`
* `redis_stream_coord_member_assigned_max_concurrency`
* `redis_stream_coord_member_runtime_max_concurrency`
* `redis_stream_coord_member_active_workers`
* `redis_stream_coord_member_current_shards`
* `redis_stream_coord_member_revoking_shards`
* `redis_stream_coord_heartbeat_total`
* `redis_stream_coord_member_expired_total`
* `redis_stream_coord_rebalance_total`
* `redis_stream_coord_rebalance_duration`
* `redis_stream_coord_scale_request_total`
* `redis_stream_coord_scale_request_failed_total`
* `redis_stream_coord_consumer_concurrency_update_total`
* `redis_stream_coord_producer_routing_request_total`
* `redis_stream_coord_migration_active`
* `redis_stream_coord_migration_active_age_seconds`
* `redis_stream_coord_revoke_pending`
* `redis_stream_coord_invariant_violations`
* `redis_stream_coord_invariant_violation_total`
* `redis_stream_coord_tick_total`
* `redis_stream_coord_tick_duration`
* `redis_stream_coord_state_conflict_total`
* `redis_stream_coord_consumer_shard_last_delivered_ms`
* `redis_stream_coord_consumer_shard_last_delivered_seq`
* `redis_stream_coord_consumer_shard_last_acked_ms`
* `redis_stream_coord_consumer_shard_last_acked_seq`
* `redis_stream_coord_consumer_shard_pending`
* `redis_stream_coord_consumer_shard_progress_updated_at_seconds`
* `redis_stream_coord_consumer_shard_progress_age_seconds`

Consumer shard progress is reported by heartbeat and validated against coordinator-owned assignment before it is stored. Redis Stream ids are split into numeric millisecond and sequence gauges so Prometheus can scrape them; the full id remains available through the monitoring API. Member lease age, heartbeat age, worker capacity, assigned shard count, revoking shard count, stream shard length, produced rate, estimated consumed rate, end offset, group offset, consumer offset, pending count, lag, and producer routing request counters are exported from the coordinator so consumer and producer libraries do not need to publish their own Micrometer meters. Message handler duration, retry, DLQ, idempotency, and duplicate-processing metrics remain application data-plane concerns.

The coordinator server exposes Prometheus-format metrics through Spring Boot Actuator at `/actuator/prometheus` when the Prometheus registry is present. The repository-provided Docker smoke stack includes Prometheus and Grafana provisioning so open-source users can run the coordinator, sample producer/consumer pods, metric scraping, and a dashboard with one command. Grafana should not embed the custom monitoring console by iframe; it should call coordinator monitoring APIs directly through a Grafana-managed datasource. Coordinator API credentials belong to Grafana datasource provisioning and should not be hard-coded into dashboard panel URLs.

Grafana shard/group row는 `producedPerSecond`, `consumedPerSecond`도 제공한다. `producedPerSecond`는 두 번의 monitoring observation 사이 Redis Stream length 증가량으로 계산한다. `consumedPerSecond`는 `streamLengthDelta - lagDelta` 기반의 추정값이므로 Redis lag가 알려져 있을 때만 계산한다. 첫 observation은 비교 대상이 없으므로 `null`을 반환한다.

## Redis Command Template Boundary

Coordinator Redis calls are centralized behind a command template instead of being scattered across state store, mutex, audit, and stream provisioning code. This keeps Lua scripts, lock operations, list/set access, health ping, and stream provisioning commands discoverable in one place and makes Redis command compatibility easier to review.

## Logs

Structured log fields:

* `streamPrefix`
* `consumerGroup`
* `coordinatorId`
* `groupEpoch`
* `assignmentEpoch`
* `memberId`
* `memberEpoch`
* `eventType`
* `reason`
* `requestedBy`
* `reshardingId`

Admin audit event는 create, delete, scale, consumer concurrency update, rollback 같은 control-plane mutation마다 남긴다. 기본 sink는 structured application log이다. `coordinator.audit.sink=redis`이면 coordinator는 JSON audit event를 다음 group-scoped key에 쓴다.

```text
redis-stream:coord:{streamPrefix:consumerGroup}:admin:audit
```

Audit event에는 다음 값을 포함한다.

* action,
* outcome,
* HTTP status,
* authenticated principal,
* granted roles,
* `requestedBy`,
* `reason`,
* request id,
* client address,
* user agent,
* route and query string,
* request duration,
* stream prefix,
* consumer group,
* resharding id when present,
* request summary,
* SHA-256 request body fingerprint,
* coordinator id,
* timestamp.

Audit log는 runtime evidence이다. Terraform/GitOps change management와 역할이 다르다. Terraform은 의도한 desired state와 승인 이력을 보여주고, coordinator audit log는 실제 coordinator가 받은 요청과 응답 결과를 보여준다. 실패한 요청, forbidden 요청, retry된 요청도 coordinator audit log에 남아야 한다.

## Alerts

* coordinator API unavailable
* group epoch은 증가했지만 assignment epoch이 따라오지 않음
* member expired 급증
* revoke pending 지속 증가
* invariant violation 발생
* active migration 장기 지속
