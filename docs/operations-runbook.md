# Operations Runbook

This runbook covers the coordinator control plane. Application message handling, retry, DLQ, and idempotency remain in the consumer application.

## Health Checks

Issue an operator token. Tokens expire after seven days by default.

```bash
read -rsp 'Coordinator password: ' RSC_PASSWORD
echo
RSC_TOKEN="$(
  curl -sS -H 'Content-Type: application/json' \
    -X POST http://localhost:8080/coord/v1/auth/login \
    -d "{\"username\":\"admin\",\"password\":\"${RSC_PASSWORD}\"}" |
  jq -r '.accessToken'
)"
unset RSC_PASSWORD
```

Coordinator health:

```bash
curl -H "Authorization: Bearer ${RSC_TOKEN}" \
  http://localhost:8080/coord/v1/monitoring/health
```

Interpretation:

| Field | Meaning |
| --- | --- |
| `status=UP` | Coordinator HTTP API and configured dependencies are healthy. |
| `status=DEGRADED` | Coordinator is up but Redis health is down. |
| `redis=NOT_CONFIGURED` | Redis is not required by the active coordinator configuration, or no Redis connection factory is configured. |

## Common Operator Checks

List groups:

```bash
curl -H "Authorization: Bearer ${RSC_TOKEN}" \
  http://localhost:8080/coord/v1/monitoring/groups
```

Inspect assignments:

```bash
curl -H "Authorization: Bearer ${RSC_TOKEN}" \
  http://localhost:8080/coord/v1/monitoring/streams/orders/groups/orders-consumer/assignments
```

Inspect migrations:

```bash
curl -H "Authorization: Bearer ${RSC_TOKEN}" \
  http://localhost:8080/coord/v1/monitoring/streams/orders/groups/orders-consumer/migrations
```

## Terraform And GitOps Admin Mutations

Production admin mutations should be applied through Terraform or another GitOps workflow when possible.

Recommended pattern:

1. Review desired changes in a pull request.
2. Run plan against coordinator read APIs.
3. Apply with a dedicated `WRITE` principal.
4. Send `X-Request-Id` and request body fields `requestedBy` and `reason`.
5. Verify coordinator audit logs and monitoring APIs after apply.

Terraform manages desired state such as group existence and shard count. It does not manage consumer runtime concurrency, heartbeats, current assignments, revoke progress, offsets, pending entries, or message payloads.

Coordinator audit remains required even when Terraform is the caller. It records the actual API request, outcome, status, principal, roles, request id, request body fingerprint, client address, duration, stream prefix, consumer group, and operation reason.

## Alerts

Alert on:

* coordinator health `DEGRADED`
* Redis connection failures
* `redis_stream_coord_member_expired_total` spike
* `redis_stream_coord_member_heartbeat_age_seconds` approaching the member lease TTL
* `redis_stream_coord_member_lease_remaining_seconds` repeatedly reaching zero for active members
* `redis_stream_coord_revoke_pending` remaining non-zero longer than the application rebalance timeout
* `redis_stream_coord_invariant_violation_total` increasing
* `redis_stream_coord_state_conflict_total` increasing rapidly
* `redis_stream_coord_consumer_shard_pending` remaining high for a member/shard
* `redis_stream_coord_consumer_shard_progress_age_seconds` becoming stale while the member is active
* `redis_stream_coord_producer_routing_request_total{status="ERROR"}` increasing
* active migration age exceeding the expected drain window
* repeated `429` responses from admin automation

## Rebalance Triage

1. Check member liveness with the members monitoring API.
2. Check assignments and `revokeProgress`.
3. If a member is stuck revoking shards, inspect that application instance for slow handlers or blocked shutdown.
4. If the rebalance timeout expires, the coordinator fences the stuck member and allows reassignment to continue.
5. Consumers that receive `FENCED_MEMBER_EPOCH` must stop local ownership and rejoin with `memberEpoch=0`.

## Migration Triage

1. Confirm producer routing metadata points to the expected `shardCount`.
2. Confirm live consumers have converged to target assignments.
3. Check whether removed shards are still reported in `currentAssignments` or `revokeProgress`.
4. If the active migration is unsafe, use the rollback API while rollback is still allowed.

## Shard Scale Procedure

For at-least-once producer workloads that tolerate duplicates, shard scale-out/in can be performed online through the coordinator scale API.

For duplicate-sensitive workloads:

1. Pause producers for the target `streamPrefix` and `consumerGroup`.
2. Wait until in-flight `XADD` calls and publish retry windows are drained.
3. Call the coordinator scale API.
4. Wait for producer routing metadata to expose the new `shardCount`.
5. Refresh producer routing caches.
6. Resume producers.

This is required because the same event id can be published to old and new shard counts if scaling occurs while produce retries are still active. The project does not provide global deduplication or a single-processing guarantee.

## Upgrade Procedure

1. Read the release notes and compatibility matrix.
2. Confirm the coordinator supports the coordination version range used by existing consumers.
3. Back up coordinator Redis metadata keys.
4. Deploy the new coordinator version.
5. Verify `/coord/v1/monitoring/health`.
6. Roll consumer applications gradually.
7. Watch member expiry, rebalance duration, revoke pending, and invariant metrics.

## Redis Metadata Backup

Coordinator metadata lives in one Redis hash key per group:

```text
redis-stream:coord:{streamPrefix:consumerGroup}:metadata
```

Back up coordinator metadata keys before schema-changing upgrades and before manual repair operations.

## Metadata Durability

The Redis metadata key is the coordinator source of truth for a group. The coordinator treats client-reported versions as observations only. If a consumer reports a higher `metadataVersion` than Redis currently stores, the coordinator asks consumers to synchronize down to the current Redis metadata with retry-safe `SYNC_METADATA` instead of trusting the client version. After the consumer reports the current version, it receives `REVOKE_PENDING` while revoke/drain is still blocking handoff and receives `OK` only when newly assigned shards may be started.

Recommended production controls:

1. Use managed Redis persistence and backups appropriate for the deployment.
2. Keep coordinator metadata keys under a dedicated `coordinator.store.key-prefix`.
3. Do not let application runtime users delete coordinator metadata keys.
4. Back up coordinator metadata keys before schema-changing upgrades or manual maintenance.
5. Treat Redis restore to an older backup as disaster recovery, not as a normal retry path.

Watch for metadata rollback signals:

1. A consumer heartbeat reports a higher previously seen `metadataVersion`, `assignmentEpoch`, or `memberEpoch` than the coordinator currently stores.
2. A producer routing cache has observed a higher `metadataVersion` than the coordinator returns.
3. Coordinator metrics report metadata version regression or store revision regression.
4. Rebalance state appears to move backward, such as a shard returning from released to revoking without a new assignment epoch.

Redis metadata has an important limitation: if Redis rolls back and no surviving consumer or producer can report the higher observed version, the rollback can be invisible to the coordinator.

If a source-of-truth metadata key is lost or Redis is restored to an older backup:

1. Stop admin mutations for the affected group.
2. Confirm whether the key was deleted, corrupted, or restored from an older backup.
3. Restore metadata from backup when possible.
4. If backup is unavailable or older than the highest client-observed version, explicitly recreate the group with the expected shard count and treat consumers/producers as a new group lifecycle.
5. Do not rely on consumer heartbeats, producer routing caches, or stale local state to reconstruct group metadata automatically.
6. Do not force clients to downgrade to a lower metadata version; fail closed until repair or recreation is complete.
7. Do not repair by only incrementing the rolled-back version number. The lost transition contents may include drain, release, shard scale, or routing decisions that cannot be inferred safely.
