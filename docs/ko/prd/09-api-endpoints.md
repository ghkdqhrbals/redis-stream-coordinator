# Coordinator API Endpoints

## Scope

이 문서는 Redis Stream Coordinator가 제공하는 HTTP API endpoint catalog이다. Endpoint의 목록, 인증, mutation 여부, 중복 요청 처리, 요청/응답 핵심 필드만 한 곳에 정리한다.

상세 동작은 다음 문서를 기준으로 한다.

* heartbeat/reconciliation flow: [`02-coordinator-architecture.md`](02-coordinator-architecture.md)
* group state와 assignment model: [`03-group-assignment-model.md`](03-group-assignment-model.md)
* shard scale과 stream version migration: [`04-stream-version-migration.md`](04-stream-version-migration.md)
* config, monitoring, metrics: [`06-data-config-observability.md`](06-data-config-observability.md)

## Base Contract

Base path:

```text
/coord/v1
```

Common headers:

```http
Content-Type: application/json
Accept: application/json
Authorization: Basic <base64(admin:password)>
```

Path parameters:

| Parameter | Meaning |
| --- | --- |
| `streamPrefix` | Coordinator가 관리하는 Redis Stream logical prefix. |
| `consumerGroup` | Redis Stream consumer group logical name. |
| `memberId` | member runtime이 생성한 UUID. |
| `reshardingId` | Coordinator가 생성한 stream version resharding id. |

Auth policy:

| API Area | Auth |
| --- | --- |
| Admin mutation API | Required |
| Monitoring API | Required |
| Member heartbeat API | 내부망 호출을 전제로 하되 Basic Auth 적용 가능 |

Common response fields:

| Field | Meaning |
| --- | --- |
| `requestId` | caller가 보낸 request id 또는 server가 생성한 추적 id. |
| `status` | API 처리 결과. |
| `errorCode` | 실패 시 machine-readable error code. |
| `message` | 실패 시 사람이 읽는 설명. |

Common status codes:

| HTTP Status | Meaning |
| --- | --- |
| `200 OK` | 조회 성공 또는 같은 상태를 다시 요청한 no-op 성공. |
| `201 Created` | 새 group 또는 migration 생성 성공. |
| `202 Accepted` | mutation 요청을 수락했고 coordinator loop에서 수렴 진행. |
| `400 Bad Request` | request body나 path parameter가 유효하지 않음. |
| `401 Unauthorized` | 인증 정보 없음 또는 실패. |
| `403 Forbidden` | 인증됐지만 mutation 권한 없음. |
| `404 Not Found` | group, member, migration을 찾을 수 없음. |
| `409 Conflict` | active migration 존재, 이미 존재하는 group 생성 시도, epoch conflict. |
| `422 Unprocessable Entity` | 요청은 유효하지만 현재 group state에서 수행 불가. |
| `429 Too Many Requests` | caller 또는 group 단위 rate limit 초과. |
| `500 Internal Server Error` | coordinator 내부 오류. |
| `503 Service Unavailable` | Redis store 또는 coordinator loop가 정상 동작하지 않거나 Redis state mutex를 획득하지 못함. |

## Endpoint Index

| Area | Method | Path | Purpose | Mutates State | Duplicate Request Handling |
| --- | --- | --- | --- | --- | --- |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | group 생성 | yes | existing group is `409 Conflict` |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | group metadata 조회 | no | not required |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/scale` | shard scale-out/in migration 시작 | yes | active migration or same target is rejected/no-op |
| Admin | `PATCH` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/consumer-concurrency` | server-side consumer `maxConcurrency` 변경 | yes | same policy returns current policy |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}` | migration 상태 조회 | no | not required |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}/rollback` | migration rollback 요청 | yes | current migration state decides acceptance |
| Member | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat` | member liveness/owned shard 보고 및 assignment 수신 | yes | `requestId`; effective state is `memberEpoch` + `ownedShards` |
| Monitoring | `GET` | `/coord/v1/monitoring/health` | coordinator health 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/groups` | group 목록 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}` | group 요약 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members` | member 상태 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments` | target/current assignment 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/consumption` | consumer shard별 Redis Stream progress 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations` | migration 목록/진행률 조회 | no | not required |

## Admin API

### Create Group

```http
POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}
```

Creates initial group metadata and stream version `1`. Stream version values are integers without a `v` prefix. Member startup, local YAML, producer, or consumer cannot create group metadata directly.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `initialShardCount` | no | Initial shard count. Omitted value uses coordinator `defaults.initial-shard-count`. |
| `versionPolicy` | no | Stream version naming policy. MVP default is `AUTO_INCREMENT`. |
| `consumerConcurrencyPolicy.defaultMaxConcurrency` | no | Default member consumer worker limit. Omitted value uses coordinator `defaults.consumer-max-concurrency`. |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `reason` | no | Human-readable change reason. |

Response summary:

| Field | Meaning |
| --- | --- |
| `streamPrefix` / `consumerGroup` | Created group identifier. |
| `groupEpoch` | Initial group epoch. |
| `metadataVersion` | Initial metadata version. |
| `activeWriteVersion` | Integer stream version `1`. |
| `readableVersions` | Initial readable versions. |
| `shardCount` | Stored shard count. |
| `consumerConcurrencyPolicy` | Stored server-side consumer concurrency policy. |

Duplicate request behavior:

* 같은 `{streamPrefix, consumerGroup}` metadata가 이미 있으면 `409 Conflict`로 거절한다.
* 기존 metadata 조회는 `GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}`를 사용한다.

### Get Group Metadata

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}
```

Returns group source-of-truth metadata. This is an admin/debug endpoint, not the member heartbeat assignment channel.

Response summary:

| Field | Meaning |
| --- | --- |
| `state` | Group state: `EMPTY`, `ASSIGNING`, `RECONCILING`, `STABLE`. |
| `groupEpoch` | Current group metadata epoch. |
| `assignmentEpoch` | Current target assignment epoch. |
| `metadataVersion` | Current metadata version. |
| `activeWriteVersion` | Producer write target integer stream version. |
| `readableVersions` | Member-readable integer stream versions. |
| `consumerConcurrencyPolicy` | Server-side consumer concurrency policy. |
| `activeMigration` | Active migration summary or null. |
| `targetAssignmentSummary` | Desired ownership summary. |
| `currentAssignmentSummary` | Member-reported ownership summary. |

### Get Producer Routing Metadata

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing
```

Returns the active write metadata that producers need to route partition keys to Redis Stream shards. This endpoint is read-only and does not create streams, change shard counts, or mutate group assignment.

Producer routing formula:

```text
shardIndex = routeV1(partitionKey, shardCount)
streamKey = format(streamKeyPattern, activeWriteVersion, shardIndex)
```

`routeV1` is a fixed protocol contract, not group metadata. The starter computes a 32-bit Murmur3 hash and maps it into `[0, shardCount)` using deterministic rejection sampling so `2^32 % shardCount` tail values do not create modulo bias. Future incompatible routing changes must use a new protocol/API version instead of storing per-group hash settings.

Routing is deterministic only for the returned `activeWriteVersion` and `shardCount`. After shard scale-out/in, the same partition key may route to a different stream key. The coordinator does not provide global event id deduplication across every shard and stream version.

Response summary:

| Field | Meaning |
| --- | --- |
| `metadataVersion` | Coordinator metadata version for producer cache invalidation. |
| `activeWriteVersion` | Integer stream version that producers must write to. |
| `shardCount` | Active write version shard count. |
| `streamKeyPattern` | Redis Stream key pattern with `{streamVersion}` and `{shardIndex}` placeholders. |
| `shards` | Active write version shard keys and Redis Cluster slots. |

### Scale Group

```http
POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/scale
```

Starts shard scale-out or scale-in by creating a next stream version migration. This is the only supported shard count mutation path. For duplicate-sensitive workloads, callers should quiesce producers and drain in-flight publish retries before calling this endpoint.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `targetShardCount` | yes | New shard count. Must be positive and different from current active shard count. |
| `consumerConcurrencyPolicy.defaultMaxConcurrency` | no | Optional server-side consumer worker limit update in the same metadata change. |
| `reason` | yes | Human-readable change reason. |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `deprecatedAfter` | no | Operational hint for old version deprecation window. |

Response summary:

| Field | Meaning |
| --- | --- |
| `reshardingId` | Created resharding id. |
| `fromVersion` / `toVersion` | Old/new integer stream versions. |
| `fromShardCount` / `toShardCount` | Old/new shard counts. |
| `state` | Initial migration state, usually `PREPARING`. |
| `groupEpoch` | Group epoch after metadata mutation. |
| `assignmentEpoch` | Assignment epoch before or after recompute depending on loop timing. |

Duplicate request behavior:

* active migration이 있으면 새 migration을 만들지 않고 `409 Conflict`로 거절한다.
* `targetShardCount`가 현재 active shard count와 같으면 새 migration을 만들지 않고 no-op response로 처리한다.

### Update Consumer Concurrency

```http
PATCH /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/consumer-concurrency
```

Updates member-side consumer worker limit. This does not change shard count and does not create a new stream version.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `defaultMaxConcurrency` | yes | Default consumer worker limit for members without override. |
| `memberOverrides` | no | Optional per-member-name max concurrency override. |
| `reason` | yes | Human-readable change reason. |
| `requestedBy` | yes | Operator or automation identity for audit. |

Response summary:

| Field | Meaning |
| --- | --- |
| `metadataVersion` | New metadata version. |
| `groupEpoch` | Bumped only if assignment weight policy depends on `maxConcurrency`. |
| `consumerConcurrencyPolicy` | Stored policy. |
| `affectedMembers` | Members that will receive changed `assignedMaxConcurrency` on heartbeat. |

Duplicate request behavior:

* 요청 policy가 현재 policy와 같으면 metadata를 새로 쓰지 않고 현재 policy를 반환한다.

### Get Migration

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}
```

Returns one migration state and drain progress.

Response summary:

| Field | Meaning |
| --- | --- |
| `reshardingId` | Resharding id. |
| `fromVersion` / `toVersion` | Old/new integer stream versions. |
| `fromShardCount` / `toShardCount` | Old/new shard counts. |
| `state` | `PREPARING`, `ACTIVE`, `DRAINING`, `DEPRECATED`, or rollback state. |
| `drainProgress` | Old version drain progress reported by members. |
| `revokeProgress` | Revoke ack progress for moved shards. |
| `createdAt` / `updatedAt` | Audit timestamps. |

### Rollback Migration

```http
POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}/rollback
```

Requests rollback inside the supported rollback window. Already-written messages in the new version are handled by the operational drain/replay policy.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `reason` | yes | Human-readable rollback reason. |
| `requestedBy` | yes | Operator or automation identity for audit. |

Response summary:

| Field | Meaning |
| --- | --- |
| `reshardingId` | Resharding id. |
| `state` | Rollback state. |
| `activeWriteVersion` | Active write version after rollback decision. |
| `readableVersions` | Readable versions after rollback decision. |
| `groupEpoch` | Group epoch after metadata update. |

Duplicate request behavior:

* migration이 이미 rollback 상태이면 현재 migration state를 반환한다.
* rollback window가 지났거나 migration state가 rollback 불가능하면 `422 Unprocessable Entity`로 거절한다.

## Member API

### Heartbeat

```http
POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat
```

Member liveness, owned shards, and revoke ack are reported through this endpoint. Coordinator assignment is returned in the same response. There is no separate assignment polling endpoint.

KIP-848 mapping:

* `GroupId` is represented by `{streamPrefix, consumerGroup}` in the HTTP path.
* `TopicPartitions` is represented by `ownedShards`.
* `Assignment.AssignedTopicPartitions` is represented by `assignment.assignedShards`.
* `Assignment.PendingTopicPartitions` is represented by `assignment.pendingShards`.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `protocolVersion` | yes | Heartbeat schema version. |
| `requestId` | yes | Request trace and retry id. |
| `memberId` | yes | Must match path parameter. Runtime-start UUID that identifies this member incarnation. |
| `memberEpoch` | yes | `0` means join/rejoin, `-1` means leave, positive value means active member epoch. |
| `rebalanceTimeoutMs` | first heartbeat required | Maximum time the coordinator waits for this member to finish revocation. |
| `metadataVersion` | yes | Member's cached metadata version. |
| `runtimeConsumerCapacity.runtimeMaxConcurrency` | yes | Process-local maximum consumer workers. Does not change server-side `maxConcurrency`. |
| `runtimeConsumerCapacity.availableConcurrency` | yes | Currently available worker capacity. |
| `ownedShards` | yes | Shards the member currently owns and may read. |
| `revokingShards` | no | Revoke/drain progress and `REVOKED` ack candidates. |
| `shardProgress` | no | Consumer-reported Redis Stream progress for assigned or revoking shards. |

Epoch validation:

* `memberEpoch=0` is accepted only for a new member or a member already fenced/expired by the coordinator.
* An active member cannot reset itself by sending `memberEpoch=0`; the coordinator rejects it as `INVALID_REQUEST`.
* Positive `memberEpoch` must match the epoch last issued by the coordinator for that member.
* A lower positive epoch is stale and returns `FENCED_MEMBER_EPOCH`.
* A higher positive epoch is not coordinator-issued and returns `INVALID_REQUEST`.
* Negative values other than the graceful leave sentinel `-1` return `INVALID_REQUEST`.

Response body:

| Field | Meaning |
| --- | --- |
| `responseTo` | Request id this response handles. |
| `status` | `OK`, `RETRY`, `UNKNOWN_MEMBER_ID`, `FENCED_MEMBER_EPOCH`, `UNSUPPORTED_PROTOCOL`, `INVALID_REQUEST`, or `GROUP_AUTHORIZATION_FAILED`. |
| `memberId` | Member id echoed by coordinator. |
| `memberEpoch` | Epoch the member must use from next heartbeat. |
| `heartbeatIntervalMs` | Server-side recommended next heartbeat interval. |
| `groupEpoch` | Latest group epoch. |
| `assignmentEpoch` | Latest target assignment epoch. |
| `metadataVersion` | Latest metadata version. |
| `assignedMaxConcurrency` | Server-side consumer worker limit assigned to this member. |
| `assignment.assignedShards` | Shards the member can read immediately. |
| `assignment.pendingShards` | Target shards blocked until previous owner releases them. |
| `assignment.metadataVersion` | Metadata version to apply with the assignment. |

Mutation behavior:

* Updates member heartbeat time and member metadata.
* Stores member owned shard report as current assignment.
* Stores revoke ack progress.
* May bump group epoch when join/leave/fencing changes membership.
* Does not read, ack, retry, or modify Redis Stream messages.

## Monitoring API

Monitoring API is read-only. State changes must go through Admin API or member heartbeat.

### Health

```http
GET /coord/v1/monitoring/health
```

Response summary:

| Field | Meaning |
| --- | --- |
| `status` | `UP`, `DEGRADED`, or `DOWN`. |
| `coordinatorId` | Coordinator server identity. |
| `redis` | Redis dependency health. Redis is checked only when Redis-backed store, Redis audit, or stream provisioning is enabled. |
| `loop` | Coordinator loop health and last tick time. |

### List Groups

```http
GET /coord/v1/monitoring/groups
```

Response summary:

| Field | Meaning |
| --- | --- |
| `groups` | Group summaries with `streamPrefix`, `consumerGroup`, state, epochs, member count, active migration flag. |

### Get Group Monitoring Summary

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}
```

Response summary:

| Field | Meaning |
| --- | --- |
| `state` | Group state. |
| `epochs` | `groupEpoch`, `assignmentEpoch`. |
| `versions` | active/readable stream versions. |
| `members` | active/expired/fenced member counts. |
| `assignments` | target/current assignment summary. |
| `migration` | active migration summary. |

### List Members

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members
```

Response summary:

| Field | Meaning |
| --- | --- |
| `members` | Member state, epochs, heartbeat age, assigned max concurrency, current assignment count, revoking count. |

### Get Assignments

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments
```

Response summary:

| Field | Meaning |
| --- | --- |
| `targetAssignment` | Desired shard owner map. |
| `currentAssignments` | Member-reported owned/revoking/revoked state. |
| `revokeProgress` | Shards blocked by revoke-before-assign dependency. |
| `invariantViolations` | Duplicate owner, missing owner, stale epoch, or unknown member references. |

### Get Consumption Progress

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/consumption
```

Response summary:

| Field | Meaning |
| --- | --- |
| `progress` | Flattened member/shard progress rows. |
| `progress[].memberId` | Consumer member reporting the progress. |
| `progress[].shard` | Stream version and shard index. |
| `progress[].streamKey` | Concrete Redis Stream key. |
| `progress[].lastDeliveredId` | Last Redis Stream id delivered to the consumer poller. |
| `progress[].lastAckedId` | Last Redis Stream id successfully acknowledged by the consumer poller. |
| `progress[].pendingCount` | Consumer-reported in-flight or pending count for the shard. |
| `progress[].updatedAt` | Time the member last updated the progress row. |

### List Migrations

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations
```

Response summary:

| Field | Meaning |
| --- | --- |
| `migrations` | Active and historical migration summaries. |
| `activeReshardingId` | Active resharding id or null. |
| `drainProgress` | Old readable version drain state. |

## Explicitly Unsupported Endpoints

The coordinator does not provide these API surfaces in the MVP:

* producer routing mutation endpoint
* member startup desired spec sync endpoint
* direct Redis Stream read/ack endpoint
* handler/retry/DLQ/idempotency marker endpoint
* assignor selection or assignor negotiation endpoint
* Kafka wire protocol endpoint
