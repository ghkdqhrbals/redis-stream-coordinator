# Coordinator Data, Configuration, and Observability

## Scope

Redis Stream Coordinator는 KIP-848 스타일의 rebalance control plane이다. 이 서버는 group metadata, member liveness, target/current assignment, migration state만 관리한다.

Coordinator config에 두지 않는 것:

* `streamPrefix`, `consumerGroup`: API path/body의 group identifier이다.
* consumer handler thread, Redis Stream read batch, block timeout, ack mode.
* pending recovery, idempotency marker lifecycle.
* producer routing cache나 member runtime local tuning.

## Redis Key Model

Coordinator가 직접 소유하는 key만 둔다.

```text
redis-stream:coord:{streamPrefix:consumerGroup}:group
redis-stream:coord:{streamPrefix:consumerGroup}:members
redis-stream:coord:{streamPrefix:consumerGroup}:member:{memberId}
redis-stream:coord:{streamPrefix:consumerGroup}:target-assignment
redis-stream:coord:{streamPrefix:consumerGroup}:current-assignment:{memberId}
redis-stream:coord:{streamPrefix:consumerGroup}:migration:active
redis-stream:coord:{streamPrefix:consumerGroup}:migration:{migrationId}
redis-stream:coord:{streamPrefix:consumerGroup}:admin:audit
```

Data-plane key는 coordinator 문서의 config에 넣지 않는다. 예를 들어 processing marker, stream read cursor, handler retry state는 member/consumer 구현 소관이다.

The group aggregate JSON stored at `...:group` includes `schemaVersion`. The current schema version is `1`. Redis-backed reads and writes reject unsupported future schema versions instead of silently downgrading or overwriting metadata from a newer coordinator. Metadata written before `schemaVersion` existed is treated as schema version `1`.

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
    # 선택 ACL이다. 비어 있으면 admin-username/admin-password가 ADMIN/MONITOR/MEMBER 전체 권한을 가진다.
    users:
      - username: admin
        password: ${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD:password}
        roles: [ADMIN]
      - username: monitor
        password: ${REDIS_STREAM_COORDINATOR_MONITOR_PASSWORD:}
        roles: [MONITOR]
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

  # Coordinator metadata store로 사용할 Redis 접속 정보이다.
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
  loop:
    # Coordinator event loop 활성화 여부이다.
    enabled: true
    # heartbeat timeout, rebalance timeout, migration drain state를 이 주기마다 평가한다.
    tick-interval: 1s
  coordination:
    state-mutex:
      # Redis-backed store에서 coordinator state 접근을 Redis mutex critical section으로 직렬화한다.
      # open source 사용자는 k8s Recreate/blue-green active-passive를 직접 맞출 필요 없이
      # 여러 coordinator pod를 띄울 수 있고, mutex를 잡은 요청만 state를 읽고 처리하고 저장한다.
      enabled: true
      # 요청 처리 중 coordinator가 죽으면 이 시간이 지난 뒤 다른 instance가 critical section을 이어받는다.
      ttl: 30s
      # mutex가 바쁠 때 heartbeat/admin/read-refresh 요청이 기다릴 최대 시간이다.
      acquire-timeout: 5s
      # mutex 획득 재시도 간격이다.
      retry-interval: 100ms

  # Admin API 요청에서 값이 생략됐을 때 적용되는 기본값이다.
  defaults:
    # create group 요청에서 initialShardCount가 없을 때 사용할 기본 shard count이다.
    initial-shard-count: 1
    # create/update 요청에서 maxConcurrency가 없을 때 member별로 내려줄 기본 consumer worker 수이다.
    consumer-max-concurrency: 1
```

Shard count와 consumer `maxConcurrency`의 실제 값은 Coordinator Admin API로 생성/변경된 group metadata에 저장한다. YAML의 `defaults`는 요청값이 생략됐을 때만 쓰이며, stream/group별 개별 설정은 Admin API로 저장한다. Kafka coordinator처럼 coordinator server config에는 shard count나 consumer concurrency min/max를 두지 않는다.

## Coordinator State Mutex

Redis-backed coordinator는 open source 배포에서 사용자가 k8s rollout 전략을 세밀하게 맞추지 않아도 되도록 Redis state mutex를 기본 사용한다.

* Redis store이면 `coordinator.coordination.state-mutex.enabled=true`가 기본이다.
* create, heartbeat, scale, rollback, consumer concurrency update, migration read, monitoring read-time operational refresh, scheduled tick은 state mutex를 획득한 instance만 수행한다.
* critical section 순서는 `acquire mutex -> read latest Redis state -> validate/process/reconcile -> save with storeRevision CAS -> release mutex`이다.
* 여러 coordinator pod가 같은 Redis store를 보더라도 동시에 state를 읽고 처리하지 않고, 요청 단위로 짧게 mutex를 잡아 직렬화한다.
* event loop tick은 mutex를 못 잡으면 조용히 skip하고, 다른 instance가 처리한다.
* `storeRevision` CAS는 mutex 이후에도 남아 stale snapshot overwrite를 막는 마지막 방어선이다.
* memory store는 개발용이며 process-local state이므로 여러 coordinator replicas에 사용하지 않는다.

이 구조의 목적은 active-active coordinator semantics가 아니라, 사용자가 `replicas=1`, `Recreate`, blue/green passive mode 같은 배포 세부사항을 직접 맞추지 않아도 안전하게 운영을 시작할 수 있게 하는 것이다.

## Access Control

MVP는 Basic Auth를 사용한다. `api.users`가 비어 있으면 `admin-username` / `admin-password`가 `ADMIN`, `MONITOR`, `MEMBER` 전체 권한을 가진다.

`api.users`가 설정되면 각 user의 role로 ACL을 평가한다.

* `ADMIN`: create, scale, consumer concurrency update, rollback 같은 mutation API.
* `MONITOR`: monitoring API와 producer routing/group/migration 조회 API.
* `MEMBER`: member heartbeat API. `authenticate-member-api=true`일 때만 요구한다.

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
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations
```

Monitoring response에는 다음 요약만 포함한다.

* group state, `groupEpoch`, `assignmentEpoch`
* active/readable stream version
* active/expired member count
* target/current assignment summary
* revoke progress
* active migration progress

## Metrics

Coordinator metric은 rebalance server 상태만 다룬다.

* `redis_stream_coord_up`
* `redis_stream_coord_group_epoch`
* `redis_stream_coord_assignment_epoch`
* `redis_stream_coord_members`
* `redis_stream_coord_heartbeat_total`
* `redis_stream_coord_member_expired_total`
* `redis_stream_coord_rebalance_total`
* `redis_stream_coord_rebalance_duration`
* `redis_stream_coord_scale_request_total`
* `redis_stream_coord_scale_request_failed_total`
* `redis_stream_coord_consumer_concurrency_update_total`
* `redis_stream_coord_migration_active`
* `redis_stream_coord_migration_active_age_seconds`
* `redis_stream_coord_revoke_pending`
* `redis_stream_coord_invariant_violations`
* `redis_stream_coord_invariant_violation_total`
* `redis_stream_coord_tick_total`
* `redis_stream_coord_tick_duration`
* `redis_stream_coord_state_conflict_total`

Message read/ack, handler duration, Redis Stream lag, pending recovery, duplicate processing metric은 member/data-plane metric이다.

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
* `migrationId`

Admin audit events are emitted for create, scale, consumer concurrency update, and rollback. The default sink is structured application logs. When `coordinator.audit.sink=redis`, the coordinator writes JSON audit events to:

```text
redis-stream:coord:{streamPrefix:consumerGroup}:admin:audit
```

## Alerts

* coordinator API unavailable
* group epoch은 증가했지만 assignment epoch이 따라오지 않음
* member expired 급증
* revoke pending 지속 증가
* invariant violation 발생
* active migration 장기 지속
