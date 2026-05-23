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

  # Admin API 요청에서 값이 생략됐을 때 적용되는 기본값이다.
  defaults:
    # create group 요청에서 initialShardCount가 없을 때 사용할 기본 shard count이다.
    initial-shard-count: 1
    # create/update 요청에서 maxConcurrency가 없을 때 member별로 내려줄 기본 consumer worker 수이다.
    consumer-max-concurrency: 1
```

Shard count와 consumer `maxConcurrency`의 실제 값은 Coordinator Admin API로 생성/변경된 group metadata에 저장한다. YAML의 `defaults`는 요청값이 생략됐을 때만 쓰이며, stream/group별 개별 설정은 Admin API로 저장한다. Kafka coordinator처럼 coordinator server config에는 shard count나 consumer concurrency min/max를 두지 않는다.

## Access Control

MVP는 Basic Auth를 사용한다. `api.users`가 비어 있으면 `admin-username` / `admin-password`가 `ADMIN`, `MONITOR`, `MEMBER` 전체 권한을 가진다.

`api.users`가 설정되면 각 user의 role로 ACL을 평가한다.

* `ADMIN`: create, scale, consumer concurrency update, rollback 같은 mutation API.
* `MONITOR`: monitoring API와 producer routing/group/migration 조회 API.
* `MEMBER`: member heartbeat API. `authenticate-member-api=true`일 때만 요구한다.

인증 실패는 `401 Unauthorized`를 반환한다. 권한이 없는 mutation 요청은 `403 Forbidden`을 반환한다.

## Monitoring API

Monitoring API는 coordinator state를 조회만 한다. 상태 변경은 Admin API로만 수행한다.

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
