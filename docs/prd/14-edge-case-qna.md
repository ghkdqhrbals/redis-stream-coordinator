# Edge Case Q&A

This page collects operational edge cases in question-and-answer form. The detailed state-machine rules remain in [Failure Modes and Edge Cases](12-failure-modes-edge-cases.md); this page is the fast path for reviewing common failure questions.

## Coordinator Availability

### Q. What happens if the coordinator disappears for a short time?

Consumers continue only within their local assignment lease. Producers continue only within their routing cache lease. When those leases expire, clients fail closed instead of reading from stale ownership or publishing with indefinitely stale routing.

### Q. What happens when a new coordinator pod starts after a crash or rolling update?

The new pod does not resume old stack frames. It loads the Redis-recorded group metadata key, acquires the Redis mutex before mutations, and advances only the workflow state already recorded in metadata.

### Q. What if two coordinator pods are alive during rolling update?

Both pods use the same Redis mutex and `storeRevision` compare-and-set. One pod commits first; the other reloads and retries or returns a retryable conflict. The design does not require users to manually guarantee a single active pod.

### Q. What if the old pod receives traffic while shutting down?

After terminating mode starts, new critical-section work returns a retryable coordinator-terminating error. Short in-flight critical sections that already own the local gate are allowed to finish so the Redis write and mutex release can complete.

## Member Expiration and Rebalance

### Q. What happens when every consumer member stops heartbeating?

The event loop marks members `EXPIRED` after the member lease TTL, bumps metadata/group epoch, clears target assignments, and moves the group to `EMPTY`. Expired member records remain temporarily for observability and stale heartbeat fencing, then stale-member cleanup can remove them.

### Q. Can a shard move to a new owner before the previous owner revokes it?

Not while the previous owner is live. The coordinator returns the shard as `pending` to the target owner until the previous owner reports `REVOKED`, expires by member lease TTL, or is fenced by rebalance timeout.

### Q. If a consumer receives DRAIN and the coordinator dies, can it resume reading?

No. Once a shard enters local revoking state, the consumer must not resume reads just because heartbeat is failing. It finishes local in-flight work and retries heartbeat with `revokingShards` until the coordinator returns a new assignment.

### Q. If a consumer dies while draining, is the shard permanently blocked?

No. The coordinator expires or fences the member after the lease or rebalance timeout. After that, the shard can be assigned according to the recorded target assignment. Duplicate processing remains possible and is part of the at-least-once guarantee boundary.

## Scale-In and Removed Shards

### Q. Is the no-live-member drain issue only a `targetShardCount=0` problem?

No. It applies to every scale-in. A `10 -> 1` scale-in can hit the same condition if every live member expires before revoke acknowledgement. The coordinator must not leave the migration stuck in `ACTIVE` only because no heartbeat target exists.

### Q. What does the coordinator do when all members expire during scale-in?

It skips the consumer-level revoke wait because there is no live member that can acknowledge revoke. The migration advances to Redis-level drain checks and completes only when every Redis consumer group on removed shard streams reports `pending=0` and known `lag=0`.

### Q. Can removed shard streams be retired just because no live member owns them?

No. No live owner only proves there is no heartbeat target left. It does not prove Redis Stream data is drained. Removed shards are retired only after Redis consumer-group evidence proves no pending entries and no remaining known lag.

### Q. What if Redis returns `lag=null` for a removed shard group?

The coordinator treats drain completion as unproven and keeps the migration in `DRAINING`. Operators must preserve `ENTRIESREAD` tracking and avoid delete/trim patterns that make Redis unable to compute lag if they want automatic drain completion.

### Q. What happens to messages that remain on shards being scaled in?

They are not moved to other shard streams by the coordinator. Existing consumers drain those records if they are live. If no consumers are live, the coordinator waits for Redis group lag and pending counts to prove the removed shard streams are drained.

### Q. What happens when `targetShardCount=0`?

Producer routing eventually returns `shardCount=0` and an empty shard list. Removed shard streams are still considered retired only after the same Redis-level drain checks used by every other scale-in.

## Producer Routing

### Q. Does the producer send heartbeats?

No. Producer routing is pull-based. The producer refreshes routing metadata from the coordinator and caches it for a bounded lease.

### Q. Does the Python producer/consumer follow different routing or heartbeat rules?

No. The Python client uses the same coordinator heartbeat statuses, logical-member split for listener concurrency, Murmur3 32-bit routing, modulo-bias removal, and `XADD NOMKSTREAM` stale-route protection as the JVM starter.

### Q. What if a producer has stale routing after scale-in?

The publisher uses Redis `XADD NOMKSTREAM` so a removed stream key is not recreated accidentally. If a stale write targets a removed shard key, the attempt fails, the routing cache is invalidated, and the producer refreshes routing before retrying.

### Q. Can the same partition key route to a different shard after resharding?

Yes. Routing determinism is scoped to the same routing protocol, same shard count, and same partition key. Changing shard count changes the routing domain.

### Q. Can the same event id be produced to two shards during resharding?

Yes. The producer provides shard-level idempotent XADD behavior, not global event-id deduplication across old and new shard layouts. Duplicate-sensitive workloads should quiesce producers and drain in-flight publish retries before shard count changes.

## Metadata and Version Correction

### Q. What if a client reports a higher metadata version than Redis currently stores?

Redis remains the source of truth. The coordinator starts a metadata correction round and responds with `SYNC_METADATA` until consumers heartbeat with the Redis-recorded metadata version.

### Q. Does the coordinator rebuild Redis metadata from consumer reports?

No. Consumer reports are not authoritative. The coordinator uses them for reconciliation and fencing, but it does not reconstruct source-of-truth metadata from stale clients.

### Q. What if Redis metadata is missing or corrupt?

The coordinator fails closed. It must not infer source-of-truth state from monitoring projections, consumer reports, or producer caches.

## Processing Guarantees

### Q. Does the project provide exactly-once processing?

No. The baseline is at-least-once. Redis Stream ACKs cannot be atomically committed with arbitrary business side effects such as database writes, Redis writes, HTTP calls, or external APIs.

### Q. Who owns idempotency and duplicate side-effect protection?

The application owns it. Use domain idempotency keys, database unique constraints, deduplication tables, compensation, or application-specific retry policies.

### Q. Does the coordinator read, ack, or delete Redis Stream messages?

No. The coordinator is the control plane. Message read, handler execution, ack/ack-delete/nack behavior, retry, DLQ, and business idempotency stay in the member data plane.
