# Operations Runbook

This runbook covers the coordinator control plane. Application message handling, retry, DLQ, and idempotency remain in the consumer application.

## Health Checks

Coordinator health:

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/health
```

Interpretation:

| Field | Meaning |
| --- | --- |
| `status=UP` | Coordinator HTTP API and configured dependencies are healthy. |
| `status=DEGRADED` | Coordinator is up but Redis health is down. |
| `redis=NOT_CONFIGURED` | Memory store or no Redis connection factory is configured. |

## Common Operator Checks

List groups:

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/groups
```

Inspect assignments:

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/streams/orders/groups/orders-consumer/assignments
```

Inspect migrations:

```bash
curl -u admin:${REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD} \
  http://localhost:8080/coord/v1/monitoring/streams/orders/groups/orders-consumer/migrations
```

## Alerts

Alert on:

* coordinator health `DEGRADED`
* Redis connection failures
* `redis_stream_coord_member_expired_total` spike
* `redis_stream_coord_revoke_pending` remaining non-zero longer than the application rebalance timeout
* `redis_stream_coord_invariant_violation_total` increasing
* active migration age exceeding the expected drain window
* repeated `429` responses from admin automation

## Rebalance Triage

1. Check member liveness with the members monitoring API.
2. Check assignments and `revokeProgress`.
3. If a member is stuck revoking shards, inspect that application instance for slow handlers or blocked shutdown.
4. If the rebalance timeout expires, the coordinator fences the stuck member and allows reassignment to continue.
5. Consumers that receive `FENCED_MEMBER_EPOCH` must stop local ownership and rejoin with `memberEpoch=0`.

## Migration Triage

1. Confirm producer routing metadata points to the expected `activeWriteVersion`.
2. Confirm live consumers have converged to target assignments.
3. Check whether old-version shards are still reported in `currentAssignments` or `revokeProgress`.
4. If the active migration is unsafe, use the rollback API before the old version is deprecated.

## Upgrade Procedure

1. Read the release notes and compatibility matrix.
2. Confirm the coordinator supports the heartbeat protocol range used by existing consumers.
3. Back up Redis metadata keys under the configured `coordinator.store.key-prefix`.
4. Deploy the new coordinator version.
5. Verify `/coord/v1/monitoring/health`.
6. Roll consumer applications gradually.
7. Watch member expiry, rebalance duration, revoke pending, and invariant metrics.

## Redis Metadata Backup

Coordinator keys use one Redis Cluster hash tag per group:

```text
redis-stream:coord:{streamPrefix:consumerGroup}:*
```

Back up all keys under `coordinator.store.key-prefix` before schema-changing upgrades.
