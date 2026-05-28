# Coordinator API Endpoints

All endpoints are under the configured base path, defaulting to `/coord/v1`.

The group identifier is always part of the API path:

```text
/{streamPrefix}/{consumerGroup}
```

## Error Format

Errors use a stable machine-readable code.

```json
{
  "code": "GROUP_ALREADY_EXISTS",
  "message": "Group already exists",
  "details": {
    "streamPrefix": "orders",
    "consumerGroup": "orders-consumer"
  }
}
```

Coordinator error codes should be managed as enums in code so API responses, tests, and docs remain consistent.

## Create Group

```http
POST /coord/v1/groups/{streamPrefix}/{consumerGroup}
Authorization: Basic ...
Content-Type: application/json
```

Request:

```json
{
  "initialShardCount": 4,
  "consumerConcurrencyPolicy": {
    "defaultMaxConcurrency": 4
  },
  "requestedBy": "operator",
  "reason": "initial deployment"
}
```

Response:

```json
{
  "streamPrefix": "orders",
  "consumerGroup": "orders-consumer",
  "groupEpoch": 1,
  "assignmentEpoch": 1,
  "activeWriteVersion": 1,
  "readableVersions": [1],
  "shardCount": 4
}
```

Conflicts:

* `GROUP_ALREADY_EXISTS`
* `INVALID_SHARD_COUNT`
* `UNAUTHORIZED`
* `FORBIDDEN`

## Heartbeat

```http
POST /coord/v1/groups/{streamPrefix}/{consumerGroup}/members/{memberId}/heartbeat
Content-Type: application/json
```

Request:

```json
{
  "protocolVersion": "1.0",
  "memberEpoch": 3,
  "assignmentEpoch": 12,
  "runtimeMaxConcurrency": 4,
  "ownedShards": [
    {
      "streamVersion": 1,
      "shard": 0,
      "lastDeliveredId": "1710000000000-0",
      "lastAckedId": "1710000000000-0",
      "pendingCount": 0
    }
  ],
  "revokingShards": [],
  "leaving": false
}
```

Response:

```json
{
  "status": "OK",
  "heartbeatIntervalMs": 1000,
  "groupEpoch": 5,
  "assignmentEpoch": 13,
  "memberEpoch": 4,
  "assignedMaxConcurrency": 4,
  "assignment": {
    "assignedShards": [
      { "streamVersion": 1, "shard": 0 }
    ],
    "pendingShards": []
  },
  "routing": {
    "activeWriteVersion": 1,
    "readableVersions": [1],
    "metadataVersion": 8
  }
}
```

Fencing statuses:

* `FENCED_MEMBER_EPOCH`
* `FENCED_STALE_OWNERSHIP`
* `FENCED_UNKNOWN_MEMBER`
* `REJOIN_REQUIRED`

## Graceful Leave

```http
POST /coord/v1/groups/{streamPrefix}/{consumerGroup}/members/{memberId}/leave
Content-Type: application/json
```

Request:

```json
{
  "memberEpoch": 4,
  "reason": "pod shutdown"
}
```

Response:

```json
{
  "status": "LEAVING",
  "groupEpoch": 6,
  "assignmentEpoch": 14
}
```

After this response, the member should stop new reads, revoke owned shards, and continue heartbeat until revocation is acknowledged.

## Producer Routing Metadata

```http
GET /coord/v1/groups/{streamPrefix}/{consumerGroup}/producer-routing
```

Response:

```json
{
  "streamPrefix": "orders",
  "consumerGroup": "orders-consumer",
  "metadataVersion": 12,
  "activeWriteVersion": 2,
  "shardCount": 8,
  "streamKeyPattern": "orders.v{version}.s{shard}",
  "hashing": {
    "strategy": "stable-default"
  }
}
```

The public API should not expose arbitrary hash algorithm and seed controls unless a real compatibility requirement appears. The producer and coordinator must share one stable routing contract.

## Start Resharding

```http
POST /coord/v1/groups/{streamPrefix}/{consumerGroup}/resharding
Authorization: Basic ...
Content-Type: application/json
```

Request:

```json
{
  "targetShardCount": 8,
  "consumerConcurrencyPolicy": {
    "defaultMaxConcurrency": 4
  },
  "requestedBy": "operator",
  "reason": "increase throughput"
}
```

Response:

```json
{
  "reshardingId": "rs-20260529-0001",
  "state": "CUTOVER",
  "fromVersion": 1,
  "toVersion": 2,
  "activeWriteVersion": 2,
  "readableVersions": [1, 2],
  "groupEpoch": 7,
  "assignmentEpoch": 15
}
```

Conflict codes:

* `RESHARDING_ALREADY_ACTIVE`
* `INVALID_TARGET_SHARD_COUNT`
* `SHARD_COUNT_UNCHANGED`
* `PROVISIONING_FAILED`

## Get Group Status

```http
GET /coord/v1/groups/{streamPrefix}/{consumerGroup}
```

Response includes:

* group metadata,
* stream versions,
* assignment summary,
* member summary,
* active resharding,
* consumer concurrency policy.

## Get Resharding Status

```http
GET /coord/v1/groups/{streamPrefix}/{consumerGroup}/resharding/{reshardingId}
```

Response includes:

* resharding state,
* old/new stream versions,
* provisioned shards,
* drain progress,
* revoke progress,
* rollback eligibility.

## Rollback Resharding

```http
POST /coord/v1/groups/{streamPrefix}/{consumerGroup}/resharding/{reshardingId}/rollback
Authorization: Basic ...
Content-Type: application/json
```

Request:

```json
{
  "requestedBy": "operator",
  "reason": "new version validation failed"
}
```

Rollback is allowed only within the configured rollback window. Messages already written to the new version must be handled by an operator-defined drain or replay policy.

## Update Consumer Concurrency

```http
PUT /coord/v1/groups/{streamPrefix}/{consumerGroup}/consumer-concurrency
Authorization: Basic ...
Content-Type: application/json
```

Request:

```json
{
  "defaultMaxConcurrency": 8,
  "requestedBy": "operator",
  "reason": "increase worker capacity"
}
```

Response includes updated policy, `metadataVersion`, and whether assignment was recalculated.

## Monitoring Endpoints

```http
GET /coord/v1/monitoring/groups/{streamPrefix}/{consumerGroup}
GET /coord/v1/monitoring/groups/{streamPrefix}/{consumerGroup}/members
GET /coord/v1/monitoring/groups/{streamPrefix}/{consumerGroup}/assignments
GET /coord/v1/monitoring/groups/{streamPrefix}/{consumerGroup}/progress
```

Monitoring endpoints are read-only and require `MONITOR` role when ACL is enabled.
