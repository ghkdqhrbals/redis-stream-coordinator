# RCP-0001: Coordinator-Managed Redis Stream Rebalance Protocol

## Status

Draft implementation on `feature/coordinator`.

## Summary

Redis Stream Coordinator moves shard ownership and rebalance decisions from
consumer clients into a coordinator service. The shape is intentionally close to
Kafka KIP-848: members heartbeat with their current epoch and owned shards, the
coordinator persists the target assignment, and consumers incrementally converge
without a stop-the-world group sync.

Reference: [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A%2BThe%2BNext%2BGeneration%2Bof%2Bthe%2BConsumer%2BRebalance%2BProtocol).

## Motivation

A single Redis Stream key can become a BigKey and is pinned to one Redis Cluster
hash slot. A production workload needs a control plane that can split a logical
stream across many stream keys, move consumers safely when capacity changes, and
change shard counts without losing ownership guarantees.

Client-side assignment alone is hard to operate because each consumer must
understand the full group state, membership changes, and stream version
migrations. A coordinator-owned assignment model makes the operational state
inspectable and gives the cluster one source of truth for fencing, migration,
and convergence.

## Goals

- Persist group metadata, member metadata, target assignments, current ownership,
  and active migrations outside of a single coordinator process.
- Support incremental shard movement when members join, leave, expire, restart,
  or change capacity.
- Fence stale members after they have already acknowledged a newer member epoch.
- Resume a pending revoke and assignment handoff after coordinator process
  replacement.
- Protect Redis-backed state updates with optimistic revision checks so multiple
  coordinators cannot silently overwrite each other.
- Publish design documents as browsable HTML separately from the code tree.

## Non-Goals

- Reimplement Redis Stream storage or consumer group internals.
- Guarantee exactly-once user processing. The protocol only controls shard
  ownership and handoff.
- Remove the need for consumers to drain in-flight records before revoking a
  shard.
- Provide a full producer SDK in this proposal.

## Public Interfaces

### Admin API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | Create a logical stream group. |
| `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}` | Read group metadata and assignment summaries. |
| `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/scale` | Start a new stream version with a target shard count. |
| `PATCH` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/consumer-concurrency` | Update capacity weights used by assignment. |
| `GET` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{migrationId}` | Inspect a migration. |
| `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{migrationId}/rollback` | Roll back an active migration. |

### Member API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat` | Join, heartbeat, report ownership, report revoke progress, or leave. |

Heartbeat statuses:

| Status | Meaning |
| --- | --- |
| `OK` | The heartbeat was accepted and includes the current assignment view. |
| `UNKNOWN_MEMBER_ID` | The group/member is not known in the requested context. |
| `FENCED_MEMBER_EPOCH` | The member has a stale or invalid epoch and must drop ownership before rejoining. |
| `UNSUPPORTED_PROTOCOL` | The client protocol version is not supported. |
| `INVALID_REQUEST` | The path member id and body member id do not match. |

### Monitoring API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/coord/v1/monitoring/health` | Coordinator and Redis health. |
| `GET` | `/coord/v1/monitoring/groups` | List known groups. |
| `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members` | Inspect member state. |
| `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments` | Inspect target/current assignments and invariant violations. |
| `GET` | `/coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations` | List migrations. |

## Protocol Model

### Group Epoch and Assignment Epoch

`groupEpoch` advances whenever membership, migration, capacity policy, or member
expiration changes the group. `assignmentEpoch` follows `groupEpoch` after
target assignment recomputation. A member can heartbeat with its last accepted
epoch until the coordinator responds with the new assignment epoch. After that
acknowledgement, an older duplicate heartbeat is fenced.

Capacity policy changes have one extra rule: if the policy update does not move
any target shard, only `metadataVersion` advances. If it does move target
assignments, `groupEpoch` and `assignmentEpoch` advance exactly once for that
rebalance.

### Member Lifecycle

| State | Description |
| --- | --- |
| `STARTING` | A member has joined but has not fully converged. |
| `ACTIVE` | A member is live and can own target shards. |
| `LEAVING` | A member is voluntarily leaving and should release ownership. |
| `EXPIRED` | The lease elapsed. The member no longer blocks reassignment. |
| `FENCED` | The member must stop using its current identity and rejoin. |

### Assignment Handoff

The coordinator computes `targetAssignments`. Members report
`currentAssignment` and `revokingShards`. A shard that moves to a new member is
returned as `pending` until the previous live owner reports it as revoked or the
previous owner expires. This prevents two active members from consuming the same
shard during normal handoff.

Each member heartbeat carries a `rebalanceTimeoutMs`. When a member keeps
owning or revoking shards that are no longer in its target assignment beyond
that timeout, the coordinator fences that member, clears its reported ownership,
and recomputes the assignment so pending shards can move forward. A member that
reports the revoke before the deadline remains active.

### Stream Version Migration

Scaling creates a new stream version instead of rewriting existing stream keys.
During an active migration, both the previous and new stream versions are
readable. Rollback removes the new version from the readable set and restores
the previous active write version.

## Operational Edge Cases

### Coordinator Dies During Rebalance

The coordinator persists group state before replying to heartbeats. A replacement
coordinator loads the same store, sees the pending revoke, and continues the
handoff from the latest target/current assignment state.

Covered by test:

- `replacement coordinator resumes pending revoke after previous coordinator stops`

### Old Consumer Returns With Stale Epoch

If a delayed consumer repeats a heartbeat with an epoch older than the member
epoch already acknowledged by the coordinator, the coordinator returns
`FENCED_MEMBER_EPOCH` and no assignment. The member must drop ownership and
rejoin with epoch `0`.

When a member joins or rejoins with epoch `0`, the coordinator ignores any
reported `ownedShards` or `revokingShards` in that heartbeat. A join starts from
empty local ownership and must converge from the coordinator response.

Covered by tests:

- `stale member epoch after acknowledged rebalance is fenced`
- `expired old consumer returning with stale ownership is fenced`
- `expired member rejoining with epoch zero does not restore stale ownership`
- `member that does not revoke moved shard is fenced after rebalance timeout`
- `member that revokes moved shard before rebalance timeout stays active`

### Race Between Coordinators

The Redis state store uses a `storeRevision` key and a Lua upsert script. A save
based on an old revision fails instead of overwriting newer state. Service
operations retry transient conflicts and return `STATE_VERSION_CONFLICT` if the
state keeps changing.

Covered by tests:

- `redis store rejects stale coordinator snapshot instead of overwriting latest state`
- service operations using `withStateConflictRetry`

### Unsupported or Old Protocol Client

A heartbeat with an unsupported protocol version returns `UNSUPPORTED_PROTOCOL`
and receives no assignment. A mismatched path/body member id returns
`INVALID_REQUEST`.

Covered by tests:

- `heartbeat with mismatched path member id is rejected`
- HTTP integration coverage for heartbeat/admin auth behavior

## Test Plan

The implementation uses TDD for newly found gaps and keeps broad scenario
coverage for operational behavior.

| Area | Coverage |
| --- | --- |
| Operational scenario matrix | 432 focused cases covering shard counts, member counts, scale up/down/rollback, churn, expiry, rolling restart, replacement, and capacity skew with descriptive scenario names. |
| Coordinator failover | Replacement coordinator resumes pending revoke and assignment after process replacement. |
| Stale consumer fencing | Old member epochs and expired owners are fenced instead of mutating current ownership. |
| Rebalance timeout | Old owners that do not revoke moved shards before their deadline are fenced so pending assignments can converge. |
| Redis state race | Optimistic revision conflict rejects stale snapshots. |
| HTTP contract | Admin/member API validation and auth behavior. |
| Redis integration | Redis-backed projection and optimistic concurrency are available as environment-gated integration tests. |

## Reread Gap Checklist

The design reread found these gaps and their current resolution status:

| Gap | Status | Notes |
| --- | --- | --- |
| Stale member heartbeat accepted after a newer epoch was already acknowledged. | Fixed in implementation. | Stale positive epochs are fenced; normal first heartbeat after a rebalance is still accepted until the new epoch is acknowledged. |
| Coordinator replacement during pending revoke was not explicitly tested. | Fixed in tests. | Shared store failover test covers the handoff continuation. |
| Expired old consumer could report stale ownership. | Fixed in implementation and tests. | Expired members with positive epochs are fenced and do not block the replacement owner. |
| Epoch-zero rejoin could restore stale ownership from the request body. | Fixed in implementation and tests. | Join/rejoin heartbeats ignore reported owned/revoking shards and start from empty local ownership. |
| Capacity policy updates could over-advance group epoch or advance it without assignment movement. | Fixed in implementation and tests. | No-move policy updates advance metadata only; assignment-moving policy updates advance the group epoch once. |
| Pending shard could wait forever if the previous owner never revoked it. | Fixed in implementation and tests. | Members now track a rebalance deadline and are fenced when they keep blocking moved shards past `rebalanceTimeoutMs`. |
| PR test comments were too verbose. | Fixed in workflow. | The PR comment now keeps totals and links only; full scenario names live in the Test HTML report. |
| Design docs lived only as implementation notes. | Fixed by process. | Markdown source is stored on the separate `design-docs` branch and rendered by a dedicated action. |
| Migration completion/deprecation flow is incomplete. | Open. | `DRAINING` and `DEPRECATED` states exist, but there is no explicit API or retention policy to finish a successful migration. |
| Always-on Redis integration in CI is incomplete. | Open. | Redis integration tests are currently gated by `REDIS_COORDINATOR_INTEGRATION_TESTS=true`; a future CI job should run a Redis service and enable them. |
| Producer routing contract is not first-class. | Open. | Producers can infer active write version from group metadata, but a dedicated route metadata endpoint/SDK is not defined. |
| Audit trail is not persisted. | Open. | Requests include `requestedBy` and `reason`, but migration/change history does not retain a full audit event log yet. |
| Operational metrics are minimal. | Open. | Health exists; rebalance latency, fenced member count, conflict retry count, and migration age should be exported. |

## Rejected Alternatives

### Client-Side Assignment Only

Rejected because every consumer would need to rebuild the full group view and
handle migration race conditions locally. This makes inspection and recovery
harder.

### In-Place Resharding

Rejected because existing Redis Stream keys cannot be repartitioned without
producer/consumer coordination. Versioned stream keys allow old and new shard
layouts to coexist during migration.

### Accept Any Lower Member Epoch

Rejected after the stale consumer reread. Accepting every lower epoch lets a
delayed heartbeat mutate ownership after the coordinator has already advanced the
member. The final rule accepts the last known epoch until a new epoch is
acknowledged, then fences older duplicates.

## Compatibility

The current member protocol version is `1`. Unsupported versions receive
`UNSUPPORTED_PROTOCOL`. Future protocol changes should add explicit version
negotiation before adding required heartbeat fields.
