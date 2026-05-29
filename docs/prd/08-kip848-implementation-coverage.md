# KIP-848 Implementation Coverage

## Purpose

This document explains which KIP-848 ideas are adapted and how they are reshaped for Redis Stream. The goal is not Kafka protocol compatibility. The goal is to apply coordinator-managed rebalance to Redis Stream shard ownership.

## Concept Mapping

| KIP-848 Concept | Redis Stream Coordinator Equivalent |
| --- | --- |
| Group Coordinator | Dedicated coordinator server |
| Member | Consumer runtime member with UUID `memberId` |
| Topic Partition | Redis Stream shard |
| Target Assignment | Coordinator-owned desired shard ownership |
| Current Assignment | Member-reported applied ownership |
| Heartbeat | Coordinator heartbeat API |
| Member Epoch | Fencing and member lifecycle generation |
| Group Epoch | Group metadata generation |
| Assignment Epoch | Target assignment calculation generation |

## Implemented Ideas

### Coordinator-Driven Rebalance

Members do not decide final ownership. The coordinator calculates target assignment and members converge through heartbeat responses.

### Declarative Target Assignment

The coordinator stores desired shard ownership separately from member-reported current ownership. This allows the coordinator to reason about pending revocations, stale owners, and assignment convergence.

### Incremental Reconciliation

The system avoids a global stop-the-world barrier. Each member reconciles assigned and revoked shards independently.

### Member Epoch Fencing

The coordinator uses member epochs to reject stale heartbeats and stale ownership reports. A fenced member must stop work and rejoin.

### Revoke-Before-Assign

A shard can move to a new live member only after the previous live owner revokes it. If the previous owner expires, the coordinator fences it and can reassign the shard.

### Server-Side Assignment

Assignment is computed and persisted by the coordinator. Consumers receive assignment in heartbeat responses and report local state back.

## Redis-Specific Changes

### Stream Versions

Kafka partitions are broker-managed. Redis Stream shards are application-managed keys. Shard count changes therefore use stream versions:

* old version becomes `DRAINING`,
* new version becomes `ACTIVE`,
* consumers read all readable versions,
* producers write only to the active version.

### Producer Routing

Kafka producers use broker metadata. Redis Stream producers use coordinator routing metadata and publish directly to Redis Stream shard keys.

### Data Plane Ownership

Kafka brokers own storage and offset state. Redis Stream Coordinator only owns control-plane metadata. Application consumers still own message handling, retries, ACK, and side effects.

### Processing Guarantees

Kafka transactions can bind Kafka output records and consumed offsets inside Kafka. Redis Stream Coordinator cannot bind arbitrary application databases or HTTP calls to Redis Stream ACK. The baseline is therefore at-least-once.

## Excluded KIP-848 Areas

* Kafka wire protocol changes.
* Broker-side offset management.
* Kafka partition protocol details.
* Kafka transactional producer semantics.
* Kafka Streams processing model.

## Residual Gaps To Decide

* Whether to support pluggable assignment strategies.
* Whether to add richer assignment diff reporting for clients.
* Whether member identity should optionally be coordinator-issued.
* Whether monitoring projections should be rebuilt automatically from an append-only audit/event log.
