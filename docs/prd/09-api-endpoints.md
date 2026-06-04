# Coordinator API Endpoints

## Scope

This document is the HTTP API catalog for Redis Stream Coordinator. It lists endpoint paths, auth expectations, mutation behavior, idempotency expectations, and the most important request and response fields.

Use the local [Scalar API Reference](../../api.html) for interactive endpoint search, schemas, examples, and curl snippets. The server also exposes a live reference at `http://localhost:8080/scalar`.
`operationId` values are treated as the API contract surface for generated clients and changed only for intentional protocol changes.

Detailed behavior is defined in these documents:

* heartbeat and reconciliation flow: [02-coordinator-architecture.md](02-coordinator-architecture.md)
* group state and assignment model: [03-group-assignment-model.md](03-group-assignment-model.md)
* shard scaling and routing: [04-resharding-routing.md](04-resharding-routing.md)
* configuration, monitoring, and metrics: [06-data-config-observability.md](06-data-config-observability.md)

## Base Contract

Base path:

```text
/coord/v1
```

Common headers:

```http
Content-Type: application/json
Accept: application/json
Authorization: Bearer <token-from-POST-/coord/v1/auth/login>
X-Request-Id: <caller-generated-id>
```

`X-Request-Id` is optional but strongly recommended for admin mutations, Terraform applies, CI automation, and incident replay. The coordinator records it in audit logs when present.

Path parameters:

| Parameter | Meaning |
| --- | --- |
| `streamPrefix` | Sharded Redis Stream prefix managed by the coordinator. Physical stream keys are built as `{streamPrefix}:{shardIndex}`. |
| `consumerGroup` | Redis Stream consumer group name. |
| `memberId` | Runtime-generated member id. The starter derives it from pod IP context by default. |
| `reshardingId` | Coordinator-generated resharding id. |

Auth policy:

| API Area | Auth |
| --- | --- |
| Admin mutation API | Required. |
| Monitoring API | Required. |
| Member heartbeat API | Optional, enabled by `coordinator.api.authenticate-member-api`. |

Common error response:

```json
{
  "status": "CONFLICT",
  "errorCode": "GROUP_ALREADY_EXISTS",
  "message": "Group already exists"
}
```

Common status codes:

| HTTP Status | Meaning |
| --- | --- |
| `200 OK` | Read succeeded or a same-state update returned the current state. |
| `201 Created` | A new group was created. |
| `202 Accepted` | A long-running mutation was accepted and reconciliation continues from metadata. |
| `400 Bad Request` | Request body or path parameters are invalid. |
| `401 Unauthorized` | Authentication is missing or invalid. |
| `403 Forbidden` | Caller is authenticated but lacks the required role. |
| `404 Not Found` | Group, member, or migration does not exist. |
| `409 Conflict` | Duplicate group, active migration, or concurrent metadata conflict. |
| `422 Unprocessable Entity` | Request is valid but not allowed in the current group state. |
| `429 Too Many Requests` | Caller or group exceeded the configured mutation rate limit. |
| `503 Service Unavailable` | Redis, stream provisioning, or coordinator state mutex is unavailable. |

## Endpoint Index

| Area | Method | Path | Purpose | Mutates State |
| --- | --- | --- | --- | --- |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}` | Create a stream shard group. | yes |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | Read group metadata. | no |
| Admin | `DELETE` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | Delete inactive group metadata. | yes |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing` | Read producer routing metadata. | no |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}/scale` | Start stream-wide shard scale-out or scale-in. | yes |
| Admin | `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}` | Read one migration. | no |
| Admin | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}/rollback` | Request migration rollback. | yes |
| Member | `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat` | Report liveness/ownership and receive assignment. | yes |
| Monitoring | `GET` | `/coord/v1/monitoring/health` | Read coordinator health. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/session` | Read monitoring principal/session context. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/compatibility` | Read coordination compatibility metadata. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/groups` | List groups. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}` | Read group monitoring summary. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members` | List members. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments` | Read target/current assignment. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/consumption` | Read consumer shard progress. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations` | List migrations. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/groups` | Read flat rows for Grafana overview. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/options/streams` | Grafana variable options for stream prefixes. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/options/consumer-groups` | Grafana variable options for consumer groups. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/options/shards` | Grafana variable options for shard indexes. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/members` | Read flat member rows for Grafana tables. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/shards` | Read flat shard status rows for Grafana. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/assignments` | Read flat assignment rows for Grafana. | no |
| Monitoring | `GET` | `/coord/v1/monitoring/grafana/messages` | Read flat stream message rows for Grafana. | no |

## Admin API

### Create Group

```http
POST /coord/v1/streams/{streamPrefix}
```

Creates initial stream shard metadata and the configured shard count. The official create path only requires `streamPrefix`; consumer groups come from consumer runtime configuration and converge through heartbeat.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `initialShardCount` | no | Initial shard count. If omitted, coordinator defaults are used. |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `reason` | no | Human-readable change reason. |

Response: `201 Created` with `StreamCreateResponse`.

Important response fields:

| Field | Meaning |
| --- | --- |
| `streamPrefix` | Created stream prefix. |
| `shardCount` | Stored shard count. |
| `metadataVersion` | Coordinator metadata version. |

Duplicate request behavior:

* If the stream prefix already has coordinator metadata, the coordinator returns `409 Conflict`.
* `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` remains only as a compatibility endpoint for older automation.

### Get Group

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}
```

Returns group source-of-truth metadata. This is an admin/debug endpoint. Consumers receive assignments through heartbeat responses.

Response: `200 OK` with `GroupResponse`.

### Delete Group

```http
DELETE /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}
```

Deletes coordinator metadata for an inactive group. Use `force=true` only for operational recovery when live members cannot leave cleanly.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `reason` | yes | Human-readable delete reason. |
| `force` | no | Delete even when live members are still registered. |

Response: `200 OK` with the deleted `GroupResponse` snapshot.

Failure behavior:

* If the group does not exist, the coordinator returns `404 Not Found` with `GROUP_NOT_FOUND`.
* If live members exist and `force=false`, the coordinator returns `422 Unprocessable Entity`.

### Get Producer Routing Metadata

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing
```

Returns read-only metadata required by producers to route partition keys to Redis Stream shard keys. This endpoint does not create streams, change shard counts, or mutate assignment.

Routing scope:

```text
shardIndex = routeV1(partitionKey, shardCount)
streamKey = format(streamKeyPattern, shardIndex)
```

Routing is deterministic only for the returned `shardCount`, routing protocol, and partition key. After shard scale-out or scale-in, the same partition key can route to a different stream key.

Response: `200 OK` with `ProducerRoutingResponse`.

Important response fields:

| Field | Meaning |
| --- | --- |
| `metadataVersion` | Coordinator metadata version for producer cache invalidation. |
| `shardCount` | Shard count producers must route against. |
| `streamKeyPattern` | Redis Stream key pattern. |
| `shards[]` | Concrete shard keys and Redis Cluster slots. |

### Scale Stream

```http
POST /coord/v1/streams/{streamPrefix}/scale
```

Starts shard scale-out or scale-in for the stream prefix. Consumer group is intentionally not part of this request. Every registered consumer group for the stream observes the changed shard set on the next heartbeat and reconciles assignment independently.

For duplicate-sensitive workloads, pause producers and drain in-flight publish retries before calling this endpoint. The project does not provide global event id deduplication across shards.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `targetShardCount` | yes | New shard count. Must be positive and different from the current active shard count. |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `reason` | yes | Human-readable change reason. |
| `deprecatedAfter` | no | Operational hint for rollback/drain timing. |

Stream-level scale only changes shard count. Consume parallelism is configured by consumer applications, for example through `@StreamListener(concurrency = N)`. The coordinator observes the resulting logical members through heartbeat and must not expose an admin endpoint that changes consumer runtime parallelism.

Compatibility note: `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/scale` remains available during the current implementation phase, but stream-scoped scale is the official operator-facing path.

Response: `202 Accepted` with `StreamScaleResponse`.

Important response fields:

| Field | Meaning |
| --- | --- |
| `streamPrefix` | Stream prefix that was scaled. |
| `targetShardCount` | Requested shard count. |
| `affectedConsumerGroups` | Consumer groups whose metadata was updated. |
| `migrations[]` | Per-group migration records. Each group converges through heartbeat responses. |

Conflict behavior:

* If another migration is active, the coordinator returns `409 Conflict` with `ACTIVE_MIGRATION_EXISTS`.
* If provisioning fails, the coordinator returns `503 Service Unavailable` with `REDIS_STREAM_PROVISIONING_FAILED`.

### Get Migration

```http
GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}
```

Returns one recorded resharding.

Response: `200 OK` with `Migration`.

### Rollback Migration

```http
POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{reshardingId}/rollback
```

Requests rollback of a migration when rollback is still allowed. Messages already written to newly added shard indexes must be handled by the application or operator drain/replay policy.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `requestedBy` | yes | Operator or automation identity for audit. |
| `reason` | yes | Human-readable rollback reason. |

Response: `202 Accepted` with `Migration`.

Failure behavior:

* If the migration does not exist, the coordinator returns `404 Not Found` with `MIGRATION_NOT_FOUND`.
* If rollback is no longer allowed, the coordinator returns `422 Unprocessable Entity` with `ROLLBACK_NOT_ALLOWED`.

## Member API

### Heartbeat

```http
POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat
```

Reports member liveness, owned shards, runtime capacity, revoke progress, and optional shard consumption progress. The coordinator returns assignment in the same response. There is no separate assignment polling endpoint.

Request body:

| Field | Required | Meaning |
| --- | --- | --- |
| `protocolVersion` | yes | Coordinator-module coordination version. |
| `requestId` | yes | Request trace and retry id. |
| `memberId` | yes | Must match the path parameter. |
| `memberName` | no | Stable logical member name for monitoring and diagnostics. |
| `memberEpoch` | yes | `0` means join/rejoin, `-1` means graceful leave, positive means active member epoch. |
| `metadataVersion` | yes | Member local metadata version. |
| `runtimeConsumerCapacity.runtimeMaxConcurrency` | yes | Process-local maximum consumer workers. |
| `runtimeConsumerCapacity.availableConcurrency` | yes | Available local worker capacity. |
| `ownedShards` | yes | Shards this member currently owns and may read. |
| `revokingShards` | no | Revoke/drain progress and `REVOKED` ack candidates. |
| `shardProgress` | no | Redis Stream progress for assigned or revoking shards. |

Response: `200 OK` with `HeartbeatResponse`.

Important response fields:

| Field | Meaning |
| --- | --- |
| `responseTo` | Request id handled by this response. |
| `status` | `OK`, `RETRY`, `SYNC_METADATA`, `REVOKE_PENDING`, `UNKNOWN_MEMBER_ID`, `FENCED_MEMBER_EPOCH`, `UNSUPPORTED_PROTOCOL`, or `INVALID_REQUEST`. |
| `memberEpoch` | Epoch the member must send on the next heartbeat. |
| `heartbeatIntervalMs` | Recommended next heartbeat interval. |
| `rebalanceTimeoutMs` | Coordinator-owned maximum revoke/drain wait before the coordinator may fence this member and reassign shards. |
| `groupEpoch` | Latest group epoch. |
| `assignmentEpoch` | Latest assignment epoch. |
| `metadataVersion` | Latest coordinator metadata version. |
| `assignment.assignedShards` | Shards the member may read immediately for `OK`; for `SYNC_METADATA` and `REVOKE_PENDING`, shards the member may keep if it is already reading them. |
| `assignment.pendingShards` | Target shards blocked by revoke-before-assign. |
| `assignment.metadataVersion` | Metadata version tied to this assignment. |

Status behavior:

| Status | Member action |
| --- | --- |
| `OK` | Apply assignment, start newly assigned shards, and continue heartbeating. |
| `RETRY` | Retry heartbeat without changing ownership. |
| `SYNC_METADATA` | Replace local metadata view with the response version, stop reads for shards not kept by `assignedShards`, do not start new shards, and send full state next heartbeat. The response is repeated until the member heartbeats with the coordinator metadata version. |
| `REVOKE_PENDING` | Metadata version is synchronized, but revoke-before-assign is still draining. Keep only already-owned `assignedShards`, keep draining revoked shards, and do not start new shards. |
| `UNKNOWN_MEMBER_ID` | Stop local work and rejoin with `memberEpoch=0`. |
| `FENCED_MEMBER_EPOCH` | Stop local work and rejoin with `memberEpoch=0`. |
| `UNSUPPORTED_PROTOCOL` | Stop. The client/server coordination version is incompatible. |
| `INVALID_REQUEST` | Stop or fail fast. The request violates the contract. |

Mutation behavior:

* Updates member liveness and lease expiration.
* Stores member owned shard reports as current assignment.
* Stores revoke ack progress.
* May bump group epoch when join, leave, expiration, or fencing changes membership.
* Does not read, ack, retry, or modify Redis Stream messages.

## Monitoring API

Monitoring endpoints are read-only. State changes must go through Admin API or member heartbeat.

### Health

```http
GET /coord/v1/monitoring/health
```

Response: `200 OK` with `HealthResponse`.

| Field | Meaning |
| --- | --- |
| `status` | `UP`, `DEGRADED`, or `DOWN`. |
| `coordinatorId` | Coordinator server identity. |
| `redis` | Redis dependency health. |
| `loop` | Coordinator loop health. |

### Monitoring Session

```http
GET /coord/v1/monitoring/session
```

Response: `200 OK` with `MonitoringSessionResponse`.

The response is intended for Grafana/API clients that need authenticated principal metadata for the monitoring calls.

| Field | Meaning |
| --- | --- |
| `authenticated` | `true` when monitoring request was authenticated by Basic Auth. |
| `username` | Principal name when available from security context. |

### Compatibility

```http
GET /coord/v1/monitoring/compatibility
```

Response: `200 OK` with coordinator-module compatibility metadata.

| Field | Meaning |
| --- | --- |
| `currentCoordinationVersion` | Current coordination version emitted by this coordinator. |
| `supportedCoordinationVersions.min/max` | Accepted coordination version range. |
| `coordinationVersions[]` | Release lifecycle metadata for each known version. |

### List Groups

```http
GET /coord/v1/monitoring/groups
```

Returns `GroupsResponse`, a list of `GroupResponse` summaries.

### Get Group Monitoring Summary

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}
```

Returns `GroupResponse` after applying operational refresh.

### List Members

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members
```

Returns `MembersResponse`.

Important fields in each member:

| Field | Meaning |
| --- | --- |
| `memberId` / `memberName` | Runtime id and logical member name. |
| `state` | `STARTING`, `ACTIVE`, `LEAVING`, `EXPIRED`, or `FENCED`. |
| `memberEpoch` | Coordinator-issued member epoch. |
| `metadataVersion` | Last metadata version reported or accepted for the member. |
| `currentAssignment` | Shards currently reported as owned. |
| `revoking` | Shards currently draining or awaiting revoke completion. |
| `shardProgress` | Last reported consumption progress. |

### Get Assignments

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments
```

Returns `AssignmentsResponse`.

| Field | Meaning |
| --- | --- |
| `targetAssignment` | Desired shard owner map. |
| `currentAssignments` | Member-reported owned shard map. |
| `revokeProgress` | Shards blocked by revoke-before-assign. |
| `invariantViolations` | Duplicate owner, missing owner, stale epoch, or unknown member references. |

### Get Consumption Progress

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/consumption
```

Returns flattened per-member, per-shard progress rows.

| Field | Meaning |
| --- | --- |
| `progress[].memberId` | Member that reported the progress. |
| `progress[].shard` | Stream version and shard index. |
| `progress[].streamKey` | Concrete Redis Stream key. |
| `progress[].lastDeliveredId` | Last Redis Stream id delivered to the consumer poller. |
| `progress[].lastAckedId` | Last Redis Stream id successfully acknowledged by the consumer poller. |
| `progress[].pendingCount` | Consumer-reported in-flight or pending count. |
| `progress[].updatedAt` | Time the member last updated this progress row. |

### List Migrations

```http
GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations
```

Returns `MigrationsResponse`.

| Field | Meaning |
| --- | --- |
| `migrations` | Active and historical migration records. |
| `activeReshardingId` | Active migration id or null. |

## Explicitly Unsupported Endpoints

The coordinator does not provide these API surfaces in the MVP:

* producer routing mutation endpoint,
* member startup desired-spec sync endpoint,
* direct Redis Stream read/ack endpoint,
* handler/retry/DLQ/idempotency marker endpoint,
* assignor selection or assignor negotiation endpoint,
* Kafka wire protocol endpoint.
