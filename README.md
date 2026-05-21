# Redis Stream Coordinator

Redis Stream Coordinator is a design project for managing Redis Stream sharding and consumer group ownership through a central coordinator.

## Why This Exists

Redis Stream stores messages under stream keys. As traffic grows, a single stream key can easily become a BigKey. Even when Redis Cluster is used, one stream key belongs to one hash slot, so the load cannot be evenly distributed across cluster nodes unless the stream is split into multiple keys.

In practice, solving this requires application-level sharding: splitting stream keys, routing producer writes, assigning shard ownership to consumers, and safely changing shard counts over time. However, there are very few public references for managing custom Redis Stream sharding specifically to avoid BigKey issues and achieve even distribution in Redis Cluster.

This project was created to fill that gap. It adapts the coordinator-managed rebalance ideas from Kafka KIP-848 to Redis Stream, using a Redis-backed coordinator as the source of truth for shard metadata, stream versions, and consumer assignments.

## Core Ideas

* Split Redis Stream data into shard keys to reduce BigKey risk.
* Design shard keys so they can be distributed evenly across Redis Cluster hash slots.
* Route producer writes using active stream version and shard routing metadata.
* Let consumer runtime members heartbeat to the coordinator and converge on coordinator-managed target assignments.
* Rebalance only the shards that need to move when members join, leave, expire, or when shard counts change.
* Handle shard count changes through next-version stream migration instead of in-place resharding.

## Documentation

The detailed design starts at [`redis-stream-coordinator/PRD.md`](redis-stream-coordinator/PRD.md).

* [Context, Goals, Non-Goals](redis-stream-coordinator/prd/01-context-goals.md)
* [Coordinator Architecture](redis-stream-coordinator/prd/02-coordinator-architecture.md)
* [Group Metadata and Assignment Model](redis-stream-coordinator/prd/03-group-assignment-model.md)
* [Stream Version Migration, Routing, and Admin API](redis-stream-coordinator/prd/04-stream-version-migration.md)
* [Member Data-Plane Boundary](redis-stream-coordinator/prd/05-processing-reliability.md)
* [Coordinator Data, Configuration, and Observability](redis-stream-coordinator/prd/06-data-config-observability.md)
* [MVP Scope, Tradeoffs, Risks, and Open Questions](redis-stream-coordinator/prd/07-mvp-risks-open-questions.md)
* [KIP-848 Implementation Coverage](redis-stream-coordinator/prd/08-kip848-implementation-coverage.md)
* [Coordinator API Endpoints](redis-stream-coordinator/prd/09-api-endpoints.md)

## Current Status

This repository currently focuses on product requirements and architecture design before implementation code. The goal is to define a practical reference for coordinator-driven shard count management, producer routing, consumer assignment, migration, and observability for Redis Stream based systems.

## License

See [LICENSE](LICENSE).
