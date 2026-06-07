# Member Data-Plane Boundary

Redis Stream Coordinator is not a message processor. It coordinates shard ownership. The application remains responsible for reading Redis Stream records, executing business logic, handling retries, and acknowledging records.

## Coordinator Guarantees

The coordinator provides:

* target shard assignment,
* revoke-before-assign sequencing,
* member lease expiration,
* stale owner fencing,
* member epoch and assignment epoch validation,
* monitoring of member-reported progress.

The coordinator does not provide:

* exactly-once business processing,
* atomic commit between Redis Stream ACK and an external database,
* global event-id deduplication,
* application retry policy,
* DLQ policy,
* handler transaction management.

## Processing Guarantee

The baseline processing model is at-least-once.

Redis Stream consumer group delivery can redeliver records through pending recovery. A consumer can crash after the handler runs but before ACK. A producer can retry after a lost response. Resharding can also create duplicate publish attempts when producer traffic is not quiesced.

Applications must assume that handlers can be invoked more than once for the same business event.

## Application Responsibilities

Applications must decide:

* whether to ACK before or after business processing,
* whether to use `XACK`, `XACKDEL`, or a custom NACK/retry flow,
* how to handle pending messages,
* how to detect duplicate business events,
* how to protect external side effects,
* when to send failed records to a DLQ.

Recommended protections:

* domain-level idempotency keys,
* database unique constraints,
* deduplication tables,
* optimistic locking,
* compensating actions,
* explicit retry and DLQ policy.

## Recommended Handler Flow

```text
XREADGROUP
  -> verify memberEpoch / assignmentEpoch / shard ownership
  -> execute business handler
  -> apply application-level duplicate guard or retry policy
  -> XACKDEL or XACK
```

If the member is fenced or loses ownership, it should stop reads for that shard and avoid ACKing records it no longer owns unless the application explicitly accepts that risk.

## Revoke Flow

When a shard disappears from `assignment.assignedShards` in a heartbeat response:

1. stop new reads for the shard,
2. wait for local in-flight work to drain or hit the application timeout,
3. report the shard in `revokingShards` with `state=REVOKED`,
4. remove the shard from local active workers.

The coordinator accepts the revoke acknowledgement and can assign the shard to the next target member.

During metadata correction, `SYNC_METADATA` and `REVOKE_PENDING` are drain-only responses. A member may keep shards it already owns and that remain in `assignment.assignedShards`, but it must not start shards that appear for the first time until a later `OK` response.

## Fencing Flow

If the coordinator returns a fencing status or rejects stale ownership:

1. stop reads and ACKs for affected shards,
2. clear local ownership state,
3. send a full heartbeat with the same `memberId`, `memberEpoch=0`, empty `ownedShards`, and empty `revokingShards`,
4. start only the shards returned by the coordinator after rejoin.

`UNKNOWN_MEMBER_ID` follows the same client-side recovery path. It means the coordinator cannot match the member to current group metadata. The consumer module must discard local ownership and request reassignment instead of repeatedly sending the stale epoch or stale ownership report.

## Guarantee Boundary Summary

| Area | Guarantee |
| --- | --- |
| Shard ownership | Coordinator-managed with revoke-before-assign for live members |
| Member liveness | Lease-based expiration |
| Handler execution | At-least-once |
| Producer publish | At-least-once under retry and resharding |
| External side effects | Application-owned |
| Global duplicate event prevention | Not provided |
