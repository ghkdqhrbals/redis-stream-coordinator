# Group Metadata and Assignment Model

## Core Entities

| Entity | Description |
| --- | --- |
| Group | A `{streamPrefix, consumerGroup}` pair managed by the coordinator |
| Stream Version | A numbered routing domain with a fixed shard count |
| Shard | The minimum Redis Stream routing and ownership unit |
| Member | A consumer runtime instance identified by `memberId` |
| Target Assignment | Coordinator-owned desired shard ownership |
| Current Assignment | Member-reported applied ownership |
| Group Epoch | Generation of group metadata changes |
| Assignment Epoch | Generation of target assignment calculation |
| Member Epoch | Generation used to fence stale member state |
| Store Revision | Optimistic write guard for Redis-backed state updates |

## Metadata Ownership

The coordinator owns:

* stream versions and shard count,
* active write version,
* readable versions,
* member registry and lease timestamps,
* target assignment,
* accepted current assignment,
* resharding state,
* consumer concurrency policy,
* audit and monitoring projections.

Members own:

* local worker lifecycle,
* actual Redis Stream reads,
* handler execution,
* ACK/NACK/XACKDEL policy,
* local progress reporting,
* local graceful shutdown.

## Assignment State

Target assignment is declarative. It records which member should own each shard once the group converges.

Current assignment is observational. It records what members have reported as applied through heartbeats.

The coordinator compares both views to enforce safety:

* a shard cannot have two live owners,
* a member cannot claim a shard that is not assigned or pending for it,
* a stale owner must be fenced before a new owner starts,
* duplicate terminal revoke reports are ignored once already accepted.

## Epochs

### Group Epoch

`groupEpoch` increments when membership or group metadata changes in a way that can affect ownership or routing:

* new member joins,
* member gracefully leaves,
* member expires,
* shard count migration changes readable versions or active write version,
* consumer concurrency policy affects assignment weight,
* rollback changes stream version state.

### Assignment Epoch

`assignmentEpoch` increments when target assignment is recalculated. If `groupEpoch` changes but assignment does not need to change, `assignmentEpoch` can remain stable.

### Member Epoch

`memberEpoch` increments when the coordinator needs to fence stale member state or acknowledge a new member lifecycle. A member that sends a heartbeat with an old epoch can be fenced and required to rejoin.

### Store Revision

`storeRevision` protects Redis state from stale overwrites. Even with the Redis state mutex, it remains useful as a final compare-and-set guard when a process resumes with an old snapshot.

## Sticky Assignment

The assignment algorithm prefers stability:

1. Keep existing owners for shards when they are still live and eligible.
2. Remove ownership from expired or leaving members.
3. Assign unowned shards to live members.
4. Balance shard counts using member capacity where configured.
5. Move the smallest possible number of shards to reduce imbalance.

The target is not perfect mathematical balance at all costs. The target is operational stability with bounded imbalance.

## Revoke-Before-Assign

When a shard must move from member A to member B:

1. Coordinator removes the shard from A's assigned set.
2. A receives the missing shard in the next heartbeat response.
3. A stops new reads and drains in-flight work.
4. A reports `revokingShards.state=REVOKED`.
5. Coordinator accepts the revoke acknowledgement.
6. Coordinator returns the shard to B as assigned.
7. B starts reading and reports ownership.

If A expires before acknowledging revoke, the coordinator fences A and can assign the shard to B.

## Membership States

| State | Meaning |
| --- | --- |
| `JOINING` | Member has started heartbeating but may not own shards yet |
| `STABLE` | Member is live and reconciled with assignment |
| `LEAVING` | Member requested graceful leave and should revoke shards |
| `EXPIRED` | Member lease timed out |
| `FENCED` | Member reported stale or invalid ownership |

## Ownership Validation

The coordinator validates every heartbeat ownership report:

* owned shards must be assigned or pending for the reporting member,
* revoking shards must match shards previously removed from assignment,
* a live member cannot claim a shard owned by another live member,
* stale member epochs are rejected,
* stale assignment epochs trigger rejoin or reconciliation,
* terminal duplicate revoke reports are ignored rather than treated as violations.

Invalid ownership reports are fenced to prevent split ownership.
