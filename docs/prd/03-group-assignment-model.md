# Group Metadata and Assignment Model

## Core Entities

| Entity | Description |
| --- | --- |
| Group | A `{streamPrefix, consumerGroup}` pair managed by the coordinator |
| Shard Count | The number of Redis Stream shard keys managed for the group |
| Shard | The minimum Redis Stream routing and ownership unit |
| Member | A logical consumer runtime member identified by `memberId` |
| Target Assignment | Coordinator-owned desired shard ownership |
| Current Assignment | Member-reported applied ownership |
| Group Epoch | Generation of group metadata changes |
| Assignment Epoch | Generation of target assignment calculation |
| Member Epoch | Generation used to fence stale member state |
| Store Revision | Durable compare-and-set guard for metadata state updates |

## Metadata Ownership

The coordinator owns:

* shard count,
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

## Logical Member and Worker Capacity

The design separates logical coordinator membership from local worker capacity.

Annotation-based consumers use Kafka-style listener concurrency: `@StreamListener(concurrency = "4")` creates four logical coordinator members inside the same application process. The starter derives the base member ID from pod IP context, then appends member suffixes such as `-m0`, `-m1`, `-m2`, and `-m3`. Each logical member has its own heartbeat loop, Redis consumer name, assignment state, member epoch, metadata version, and shard ownership. Annotation listeners do not have a single-member toggle.

Bean-based consumers can still register one coordinator member explicitly with `CoordinatorConsumerProperties.consumer(...)`. In that path, `runtimeMaxConcurrency` is local worker capacity for that one member. It controls how many handler executions or poll workers that member can run locally; it does not create additional coordinator members.

This split keeps the ownership rule simple: the coordinator assigns shards to live members, while each member decides how to multiplex its owned shards across its local worker capacity.

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
* shard count migration changes the managed shard set,
* consumer concurrency policy affects assignment weight,
* rollback changes resharding state.

### Assignment Epoch

`assignmentEpoch` increments when target assignment is recalculated. If `groupEpoch` changes but assignment does not need to change, `assignmentEpoch` can remain stable.

### Member Epoch

`memberEpoch` increments when the coordinator needs to fence stale member state or acknowledge a new member lifecycle. A member that sends a heartbeat with an old epoch can be fenced and required to rejoin.

### Store Revision

`storeRevision` protects Redis-backed group metadata from stale overwrites. It is incremented only with committed metadata updates and remains the final compare-and-set guard when a process resumes with an old snapshot.

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

## Ownership and Consumer Concurrency

Shard ownership and consumer concurrency are intentionally separate:

* a shard can have exactly one live owner at a time,
* a consumer member can own many shards,
* annotation listener `concurrency` creates logical coordinator members, not local workers inside a single member,
* bean-based `runtimeMaxConcurrency` describes local worker capacity for one member, not a hard ownership limit,
* `assignedMaxConcurrency` caps the coordinator-approved local worker capacity reported by one member,
* concurrency can be used as a balancing weight, but the coordinator must not require shard count to be less than or equal to member concurrency,
* when one member owns more shards than its worker count, the member must multiplex owned shards across local workers,
* the built-in polling adapter must keep one active Redis read loop per shard at most, even when several local workers are available.

For annotation listeners, `concurrency = 4` means four independently assigned coordinator members. For bean-based integrations, `runtimeMaxConcurrency = 4` means one member can run up to four local handler executions in parallel. Neither setting means a member can own only four shards.

If one member owns more shards than it has available local workers, the built-in polling adapter rotates across owned shards. Later shard indexes must continue to be polled and processed; assignment imbalance must not create permanent shard starvation.
