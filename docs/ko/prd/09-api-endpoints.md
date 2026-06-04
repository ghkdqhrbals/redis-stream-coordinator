# Coordinator API Endpoints

## Scope

이 문서는 Redis Stream Coordinator가 제공하는 HTTP API endpoint catalog이다. Endpoint의 목록, 인증, mutation 여부, 중복 요청 처리, 요청/응답 핵심 필드만 한 곳에 정리한다.

Endpoint 검색, schema, example, curl snippet은 [Scalar API Reference](../../api.html)를 우선 사용한다. 실제 실행 환경에서는 `http://localhost:8080/scalar`로도 확인할 수 있다. 이 Markdown 문서는 설계 관점의 API contract summary로 유지한다.
`operationId`는 생성형 클라이언트에서 사용하는 공개 API 식별자이므로, 의도적인 프로토콜 변경 시에만 변경한다.

상세 동작은 다음 문서를 기준으로 한다.

* heartbeat/reconciliation flow: [`02-coordinator-architecture.md`](02-coordinator-architecture.md)
* group state와 assignment model: [`03-group-assignment-model.md`](03-group-assignment-model.md)
* shard scale과 routing: [`04-resharding-routing.md`](04-resharding-routing.md)
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
X-Request-Id: <caller-generated-id>
```

`X-Request-Id`는 선택값이지만 admin mutation, Terraform apply, CI automation, incident replay에서는 강하게 권장한다. 값이 있으면 coordinator audit log에 기록한다.

Path parameters:

| Parameter | Meaning |
| --- | --- |
| `streamPrefix` | Coordinator가 관리하는 Redis Stream logical prefix. |
| `consumerGroup` | Redis Stream consumer group logical name. |
| `memberId` | member runtime이 생성한 member id. starter는 기본적으로 pod IP context에서 생성한다. |
| `reshardingId` | Coordinator가 생성한 resharding id. |

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
| `503 Service Unavailable` | Redis dependency, stream provisioning, coordinator loop가 정상 동작하지 않거나 state serialization을 확보하지 못함. |

## Endpoint Index

| Area | Method | Path | Purpose | Mutates State | Duplicate Request Handling |
| --- | --- | --- | --- | --- | --- |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}` | stream shard group 생성 | yes | existing stream metadata is `409 Conflict` |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | group metadata 조회 | no | not required |
| Admin | `DELETE` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | inactive group metadata 삭제 | yes | live member가 있으면 force 없이는 reject |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}/scale` | stream 전체 shard scale-out/in migration 시작 | yes | active migration or same target is rejected/no-op |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing` | producer 라우팅 메타데이터 조회 | no | not required |
| Admin | `PATCH` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/consumer-concurrency` | server-side consumer `maxConcurrency` 변경 | yes | same policy returns current policy |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}` | migration 상태 조회 | no | not required |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}/rollback` | migration rollback 요청 | yes | current migration state decides acceptance |
| Member | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat` | member liveness/owned shard 보고 및 assignment 수신 | yes | `requestId`; effective state is `memberEpoch` + `ownedShards` |
| Monitoring | `GET` | `/coord/v1/monitoring/health` | coordinator health 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/session` | 모니터링 principal/session 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/compatibility` | coordination compatibility 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/groups` | group 목록 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}` | group 요약 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members` | member 상태 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments` | target/current assignment 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/consumption` | consumer shard별 Redis Stream progress 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations` | migration 목록/진행률 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/groups` | Grafana overview의 group flat rows 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/options/streams` | Grafana stream prefix 변수 옵션 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/options/consumer-groups` | Grafana consumer group 변수 옵션 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/options/shards` | Grafana shard 옵션 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/members` | Grafana member flat rows 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/shards` | Grafana shard 상태 행 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/assignments` | Grafana assignment flat rows 조회 | no | not required |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/messages` | Grafana 메시지 행 조회 | no | not required |

## Admin API

### Create Group

```http
POST /coord/v1/streams/{streamPrefix}
```

초기 stream shard metadata와 shard count를 생성한다. 공식 create path는 `streamPrefix`만 받는다. Consumer group은 consumer runtime configuration에서 오며 heartbeat를 통해 수렴한다.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `initialShardCount` | no | Initial shard count. Omitted value uses coordinator `defaults.initial-shard-count`. |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `reason` | no | Human-readable change reason. |

Response summary:

| Field | Meaning |
| --- | --- |
| `streamPrefix` | 생성된 stream prefix. |
| `shardCount` | Stored shard count. |
| `metadataVersion` | 생성된 coordinator metadata version. |

Duplicate request behavior:

* stream prefix에 coordinator metadata가 이미 있으면 `409 Conflict`로 거절한다.
* `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}`는 이전 automation 호환용으로만 남긴다.

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
| `shardCount` | Stored shard count. |
| `consumerConcurrencyPolicy` | Server-side consumer concurrency policy. |
| `activeMigration` | Active migration summary or null. |
| `targetAssignmentSummary` | Desired ownership summary. |
| `currentAssignmentSummary` | Member-reported ownership summary. |

### Delete Group

```http
DELETE /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}
```

Inactive group의 coordinator metadata를 삭제한다. Live member가 정상 leave할 수 없는 운영 복구 상황에서만 `force=true`를 사용한다.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `reason` | yes | Human-readable delete reason. |
| `force` | no | Live member가 남아 있어도 삭제할지 여부. |

Response summary:

| Field | Meaning |
| --- | --- |
| `streamPrefix` / `consumerGroup` | 삭제된 group identifier. |
| `metadataVersion` | 삭제 직전 metadata version. |
| `shardCount` | 삭제 직전 shard count. |

Failure behavior:

* group이 없으면 `404 Not Found`와 `GROUP_NOT_FOUND`를 반환한다.
* live member가 있고 `force=false`이면 `422 Unprocessable Entity`로 거절한다.

### Get Producer Routing Metadata

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing
```

Returns the routing metadata that producers need to route partition keys to Redis Stream shards. This endpoint is read-only and does not create streams, change shard counts, or mutate group assignment.

Producer routing formula:

```text
shardIndex = routeV1(partitionKey, shardCount)
streamKey = format(streamKeyPattern, shardIndex)
```

`routeV1` is a fixed protocol contract, not group metadata. The starter computes a 32-bit Murmur3 hash and maps it into `[0, shardCount)` using deterministic rejection sampling so `2^32 % shardCount` tail values do not create modulo bias. Future incompatible routing changes must use a new protocol/API version instead of storing per-group hash settings.

Routing is deterministic only for the returned `shardCount`, routing protocol, and partition key. After shard scale-out/in, the same partition key may route to a different stream key. The coordinator does not provide global event id deduplication across every shard.

Response summary:

| Field | Meaning |
| --- | --- |
| `metadataVersion` | Coordinator metadata version for producer cache invalidation. |
| `shardCount` | Shard count producers must route against. |
| `streamKeyPattern` | Redis Stream key pattern with `{shardIndex}` placeholders. |
| `shards` | Concrete shard keys and Redis Cluster slots. |

### Scale Stream

```http
POST /coord/v1/streams/{streamPrefix}/scale
```

Stream prefix의 shard scale-out/in을 시작한다. Consumer group은 이 요청 path에 포함하지 않는다. 해당 stream을 읽는 모든 consumer group은 다음 heartbeat에서 변경된 shard set을 보고 각자 assignment를 재계산한다.

For duplicate-sensitive workloads, callers should quiesce producers and drain in-flight publish retries before calling this endpoint.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `targetShardCount` | yes | New shard count. Must be positive and different from current active shard count. |
| `reason` | yes | Human-readable change reason. |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `deprecatedAfter` | no | Operational hint for rollback/drain window. |

stream-level scale request에는 `consumerConcurrencyPolicy`를 넣지 않는다. consumer concurrency는 group runtime policy이므로 group-scoped consumer concurrency endpoint에서 별도로 관리한다.

Compatibility note: 현재 구현 단계에서는 `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/scale`도 남아 있지만, 운영자가 사용할 공식 scale path는 stream-scoped endpoint이다.

Response summary:

| Field | Meaning |
| --- | --- |
| `streamPrefix` | scale 대상 stream prefix. |
| `targetShardCount` | 요청한 shard count. |
| `affectedConsumerGroups` | metadata가 변경된 consumer group 목록. |
| `migrations[]` | group별 migration record. 각 group은 heartbeat 응답으로 수렴한다. |

Duplicate request behavior:

* active migration이 있으면 새 migration을 만들지 않고 `409 Conflict`로 거절한다.
* `targetShardCount`가 현재 active shard count와 같으면 새 migration을 만들지 않고 no-op response로 처리한다.

### Update Consumer Concurrency

```http
PATCH /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/consumer-concurrency
```

Updates member-side consumer worker limit. This does not change shard count.

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
| `fromShardCount` / `toShardCount` | Old/new shard counts. |
| `state` | `PREPARING`, `ACTIVE`, `DRAINING`, `DEPRECATED`, or rollback state. |
| `drainProgress` | Removed-shard drain progress reported by members. |
| `revokeProgress` | Revoke ack progress for moved shards. |
| `createdAt` / `updatedAt` | Audit timestamps. |

### Rollback Migration

```http
POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}/rollback
```

Requests rollback inside the supported rollback window. Already-written messages in newly added shard indexes are handled by the operational drain/replay policy.

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
| `shardCount` | Shard count after rollback decision. |
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
| `protocolVersion` | yes | Coordinator-module coordination version carried by the heartbeat request. |
| `requestId` | yes | Request trace and retry id. |
| `memberId` | yes | Must match path parameter. Runtime member id derived from pod IP context by default. |
| `memberEpoch` | yes | `0` means join/rejoin, `-1` means leave, positive value means active member epoch. |
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
| `status` | `OK`, `RETRY`, `SYNC_METADATA`, `REVOKE_PENDING`, `UNKNOWN_MEMBER_ID`, `FENCED_MEMBER_EPOCH`, `UNSUPPORTED_PROTOCOL`, or `INVALID_REQUEST`. |
| `memberId` | Member id echoed by coordinator. |
| `memberEpoch` | Epoch the member must use from next heartbeat. |
| `heartbeatIntervalMs` | Server-side recommended next heartbeat interval. |
| `rebalanceTimeoutMs` | Coordinator-owned maximum revoke/drain wait before the coordinator may fence this member and reassign shards. |
| `groupEpoch` | Latest group epoch. |
| `assignmentEpoch` | Latest target assignment epoch. |
| `metadataVersion` | Latest metadata version. |
| `assignedMaxConcurrency` | Server-side consumer worker limit assigned to this member. |
| `assignment.assignedShards` | `OK`에서는 즉시 read 가능한 shard이다. `SYNC_METADATA`와 `REVOKE_PENDING`에서는 이미 읽고 있던 shard 중 계속 유지 가능한 shard이다. |
| `assignment.pendingShards` | Target shards blocked until previous owner releases them. |
| `assignment.metadataVersion` | Metadata version to apply with the assignment. |

Status behavior:

| Status | Member action |
| --- | --- |
| `OK` | Assignment을 적용하고 신규 assigned shard read를 시작한다. |
| `RETRY` | Ownership을 바꾸지 않고 full state heartbeat를 재시도한다. |
| `SYNC_METADATA` | Local metadata version을 response version으로 교체하고, 유지 가능한 assigned shard 외에는 read를 중단한다. 신규 shard read는 시작하지 않는다. Consumer가 coordinator metadata version으로 heartbeat할 때까지 반복될 수 있다. |
| `REVOKE_PENDING` | Metadata version은 맞았지만 revoke-before-assign handoff가 아직 끝나지 않았다. 기존 owned shard 중 assigned에 남은 shard만 유지하고 revoke/drain을 계속하며 신규 shard read는 시작하지 않는다. |
| `UNKNOWN_MEMBER_ID` | Local work를 멈추고 `memberEpoch=0`으로 rejoin한다. |
| `FENCED_MEMBER_EPOCH` | 모든 local work를 멈추고 `memberEpoch=0`으로 rejoin한다. |
| `UNSUPPORTED_PROTOCOL` | Client/server coordination version이 호환되지 않으므로 중단한다. |
| `INVALID_REQUEST` | Contract 위반이므로 fail fast한다. |

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

### Monitoring Session

```http
GET /coord/v1/monitoring/session
```

Response summary:

| Field | Meaning |
| --- | --- |
| `authenticated` | 모니터링 호출이 Basic Auth로 인증된 경우 `true`. |
| `username` | SecurityContext에서 추출 가능한 사용자명. |

### Compatibility

```http
GET /coord/v1/monitoring/compatibility
```

Response summary:

| Field | Meaning |
| --- | --- |
| `currentCoordinationVersion` | Current coordinator module이 사용하는 coordination version. |
| `supportedCoordinationVersions.min/max` | Coordinator가 수용하는 coordinator-module coordination version range. |
| `coordinationVersions[]` | 각 coordination version의 release lifecycle entry. |
| `coordinationVersions[].version` | Coordination version number. |
| `coordinationVersions[].introducedIn.major/minor/patch` | Version이 처음 도입된 release. |
| `coordinationVersions[].minimumSupportedUntil.major/minor/patch` | 이 release 전까지 version을 제거하지 않는 최소 지원 보장. |

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
| `versions` | stored shard count. |
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
| `drainProgress` | Removed shard drain state. |

## Explicitly Unsupported Endpoints

The coordinator does not provide these API surfaces in the MVP:

* producer routing mutation endpoint
* member startup desired spec sync endpoint
* direct Redis Stream read/ack endpoint
* handler/retry/DLQ/idempotency marker endpoint
* assignor selection or assignor negotiation endpoint
* Kafka wire protocol endpoint
