# Coordinator Implementation Plan

Last updated: 2026-05-22

This document tracks what has been implemented, what is verified by tests, and what should be built next for the Redis Stream Coordinator module.

Status legend:

* [x] Done: implemented and covered by local tests.
* [ ] Planned: not implemented yet.
* [~] Partial: implemented enough for the MVP, but still missing production behavior.

## Current Snapshot

The project contains a Spring Boot 4 / Kotlin / Java 24 module named `coordinator-server`.

The module currently acts as a coordinator control plane. It manages group metadata, member heartbeats, sticky shard assignment, migration state, and Redis-backed coordinator metadata persistence. It does not yet provision Redis Stream keys or Redis consumer groups.

## Phase 1: Project Foundation

* [x] Gradle multi-module project.
* [x] Gradle Wrapper 8.14.5.
* [x] Spring Boot 4.0.6.
* [x] Kotlin 2.2.21.
* [x] Java 24 toolchain.
* [x] Foojay toolchain resolver.
* [x] IntelliJ setup guide.

## Phase 2: Coordinator APIs

* [x] Admin group creation API.
* [x] Admin group read API.
* [x] Scale API.
* [x] Consumer concurrency update API.
* [x] Migration lookup API.
* [x] Migration rollback API.
* [x] Member heartbeat API.
* [x] Monitoring health, groups, members, assignments, and migrations APIs.
* [x] Basic Auth for admin and monitoring APIs.
* [x] Optional Basic Auth for member heartbeat API.

## Phase 3: Coordination Semantics

* [x] Group metadata with `groupEpoch`, `metadataVersion`, and `assignmentEpoch`.
* [x] Initial stream version and readable version tracking.
* [x] Member join/rejoin through heartbeat.
* [x] Graceful leave through heartbeat.
* [x] Member lease expiration.
* [x] Sticky assignment with weighted balancing.
* [x] `assignedShards` and `pendingShards` split.
* [x] Revoke-before-assign enforcement.
* [x] Capacity-policy-driven rebalance epoch advancement.
* [x] Expired/stale owner fencing scenarios.
* [x] Rebalance timeout fencing for stuck owners.
* [x] Scale creates a next stream version and keeps old/new versions readable.
* [x] Active migration rollback.
* [ ] Automatic migration drain completion and `DEPRECATED` transition.
* [ ] Full producer-side route metadata contract.

## Phase 4: Redis Cluster Environment

* [x] Three-node Redis Cluster Docker Compose.
* [x] Cluster init container.
* [x] All 16384 slots assigned.
* [x] Host-reachable Redis Cluster redirects.
* [x] Spring Redis Cluster configuration.
* [x] Lettuce node address mapping for Docker-to-host access.
* [x] Redis health status in coordinator health response.
* [x] Gated Redis integration tests.

## Phase 5: Redis-Backed Coordinator Store

* [x] `CoordinatorStateStore` abstraction.
* [x] In-memory implementation.
* [x] Redis implementation.
* [x] Redis aggregate group key.
* [x] Redis projected keys for members, target assignments, current assignments, migrations, and active migration.
* [x] Group-scoped keys use a shared Redis Cluster hash tag so Lua updates stay single-slot.
* [x] Lua upsert replaces aggregate and projections atomically.
* [x] `storeRevision` compare-and-set detects stale writes.
* [x] Coordinator mutation retry on transient state conflicts.
* [ ] Redis admin audit log.

## Phase 6: Redis Stream Shard Operations

* [x] Stream shard key format helper: `streamPrefix:v{version}:shard:{index}`.
* [x] Redis Cluster hash-slot calculator using Redis hash-tag behavior.
* [x] Equal-master-range distribution helper for planning shard spread.
* [x] Validation for unsafe stream prefixes, stream versions, and shard counts before key generation.
* [ ] Create stream shard keys during group creation.
* [ ] Create Redis consumer groups for each shard during group creation.
* [ ] Create next-version stream shard keys during scale.
* [ ] Create Redis consumer groups for next-version shards during scale.
* [ ] Add Redis integration tests for live stream provisioning.

## Phase 7: GitHub Automation

* [x] Codex review workflow using `OPENAI_API_KEY`.
* [x] Review prompt in `prompts/review.md`.
* [x] PR test result workflow with read-only test job permissions.
* [x] Separate PR comment job with comment write permission.
* [x] Design docs preview on PR.
* [x] Manual design docs publish to `gh-pages`.

## Phase 8: Operations and Observability

* [ ] Structured audit logs.
* [ ] Micrometer metrics from the PRD.
* [ ] Invariant violation metric.
* [ ] Active migration age metric.
* [ ] Member expiration metric.
* [ ] Admin API rate limiting.
* [ ] Operational runbook.

## Current Test Coverage

Local test command:

```bash
./gradlew :coordinator-server:test
```

Covered now:

* Coordinator unit behavior for membership, expiration, assignment, scale, rollback, stale epochs, and rebalance timeout.
* Operational scenario matrix for shard/member/capacity/migration combinations.
* HTTP integration for auth, validation, group creation, heartbeat, and monitoring assignments.
* State store tests for memory store, Redis key model, Redis state projection, and stream shard key planning.
* Gated Redis integration tests for Redis-backed aggregate/projection persistence.
* Spring application context load.

## Next Implementation Plan

1. Implement a Redis Stream provisioning component.
   * Input: `GroupMetadata` active/readable versions and generated shard keys.
   * Create stream keys with a minimal bootstrap entry only if needed.
   * Create consumer groups idempotently with `XGROUP CREATE ... MKSTREAM`.
   * Treat `BUSYGROUP` as success.

2. Wire provisioning into group create and scale.
   * On group create, provision version 1 shard keys and consumer groups.
   * On scale, provision the next version before exposing it as `activeWriteVersion`.
   * If provisioning fails, do not publish partially updated coordinator metadata.

3. Add producer routing metadata API.
   * Return active write version, shard count, hash algorithm, hash seed, and shard key pattern.
   * Include metadata version for client-side cache invalidation.

4. Add live Redis integration tests for stream provisioning.
   * Verify stream keys exist after group create.
   * Verify consumer groups exist on each shard.
   * Verify next-version streams and consumer groups exist after scale.
   * Verify idempotent retry behavior.

5. Add migration drain completion.
   * Detect when old-version shards have no active owners and no revoke blockers.
   * Transition migration to `DEPRECATED`.
   * Remove old readable version after the drain condition is met.

## Open Risks

* Redis Stream data-plane provisioning is not implemented yet, so the coordinator currently assigns metadata shards but does not create the backing Redis Stream resources.
* Migration completion is still manual/implicit through rollback only; automatic drain-to-deprecated behavior is not implemented.
* The Redis-backed store has stale-write protection, but broader multi-process stress tests are still needed.
* Metrics, audit logs, and rate limiting are still planned work.
