# Implementation Status

Last updated: 2026-06-07

## Snapshot

The repository currently contains three production Gradle modules:

| Module | Status | Purpose |
| --- | --- | --- |
| `coordinator-server` | MVP implemented | Spring Boot control plane for group metadata, heartbeat reconciliation, assignment, migration, monitoring, ACL, audit, centralized metrics, and Redis-backed state. |
| `redisstream-core` | Implemented | Shared coordination version contract and versioned timing defaults for coordinator and support modules. |
| `com.redisstream:redisstream-spring-boot-starter` | MVP implemented | Spring Boot integration layer for consumer heartbeat lifecycle, shard callbacks, Redis Stream polling, producer routing, publishing, graceful leave, and coordinator progress reporting. |

Overall status:

| Area | Status | Notes |
| --- | --- | --- |
| Project foundation | Done | Gradle Kotlin DSL, Gradle Wrapper `8.14.5`, Spring Boot `4.0.6`, Kotlin `2.2.21`, Java toolchain `24`, Foojay resolver. |
| Coordinator API | Done | Admin, member heartbeat, producer routing, migration, rollback, and monitoring endpoints are implemented. |
| Rebalance semantics | Done for MVP | Sticky assignment, revoke-before-assign, member join/rejoin/leave/expiry, stale ownership fencing, rebalance timeout, scale-in Redis drain checks, and monitoring refresh conflict retry are implemented. |
| Redis state store | Done | Memory and Redis stores are available. Redis state access uses a distributed mutex, store revision compare-and-set, schema version guard, and Lua metadata-hash updates. |
| Redis Stream shard provisioning | Done | Optional stream/consumer-group provisioning is implemented and gated by config. Idempotent retry and partial failure behavior are covered. |
| Security and audit | Done for MVP | Basic Auth, role ACL, structured audit logs, optional Redis audit sink, and per-caller/group admin mutation rate limiting are implemented. |
| Observability | Done for MVP | Coordinator Micrometer/Prometheus metrics, monitoring APIs, consumer shard progress, shard offset/lag gauges, the built-in monitoring console, and local Prometheus/Grafana provisioning are implemented. Starter modules do not publish their own Micrometer meters. |
| Consumer starter | Done for MVP | Heartbeat lifecycle, shard callbacks, runtime capacity/progress reporting, runtime concurrency enforcement, fencing/rejoin, pending/revoking handling, graceful leave, and opt-in Redis polling adapter are implemented. |
| Producer starter | Done for MVP | Producer routing cache, routing validation, stale-cache invalidation after write failure, opt-in publish retry, Redis Stream publisher, payload helper, and batch publish are implemented. |
| Processing guarantee | Done for MVP | Public guarantee is at-least-once. Single-processing guarantees are not provided because application business side effects cannot be atomically committed with Redis Stream ACKs. |
| Docker distribution | Ready for MVP | Dockerfile, external-Redis pod/stress Compose files, PR smoke test, manual GHCR publish workflow, and user guide are implemented. First public image release remains. |
| Open source operations | Ready for MVP | Contributing guide, security policy, changelog, testing guide, Docker guide, and operations runbook are available. |

## Verified Commands

The current implementation has been verified with:

```bash
./gradlew :coordinator-server:test --tests '*CoordinatorMetricsTest' --tests '*CoordinatorServiceTest' --no-daemon
./gradlew :redisstream-spring-boot-starter:test --no-daemon
./gradlew :redisstream-spring-boot-starter:test :coordinator-server:test --no-daemon
./gradlew test build --no-daemon
python3 .github/scripts/test_docker_distribution.py
docker build -t redis-stream-coordinator/coordinator-server:jvm-ci .
```

Redis integration tests are gated and require an external Redis Cluster:

```bash
export AWS_REDIS_CLUSTER_NODES=3.39.42.28:6379
export AWS_REDIS_PASSWORD='your-redis-password'
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test --tests '*RedisCoordinatorStateStoreIntegrationTest'
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test --tests '*RedisStreamProvisioningIntegrationTest'
```

## Implemented

### Coordinator Server

* [x] Admin group creation API.
* [x] Group metadata read API.
* [x] Producer routing metadata API.
* [x] Shard scale API.
* [x] Migration lookup API.
* [x] Migration rollback API.
* [x] Member heartbeat API.
* [x] Monitoring health, group, member, assignment, and migration APIs.
* [x] Monitoring consumption progress API.
* [x] Built-in monitoring console at `/console`.
* [x] Shared error enum for HTTP status, error code, and default message.
* [x] Scheduled coordinator event loop for lease expiry, rebalance timeout, and migration drain progress.
* [x] Redis-backed distributed state mutex so multiple coordinator pods can be deployed without user-managed single-active rollout rules.

### Coordination Semantics

* [x] Group metadata model with `groupEpoch`, `metadataVersion`, and `assignmentEpoch`.
* [x] Heartbeat protocol compatibility range.
* [x] Coordinator-issued member epoch validation.
* [x] Join/rejoin with `memberEpoch=0`.
* [x] Graceful leave with `memberEpoch=-1`.
* [x] Member lease expiry and fencing state.
* [x] Stale ownership report validation and fencing for unauthorized `ownedShards` or non-terminal `revokingShards`.
* [x] Sticky assignment with balancing by live logical member count.
* [x] `assignedShards` and `pendingShards` split.
* [x] Revoke-before-assign handoff.
* [x] Rebalance timeout fencing.
* [x] Scale migration with updated shard count.
* [x] Old/new readable shard set during active migration.
* [x] Automatic migration drain and `DEPRECATED` transition.
* [x] Scale-in completion through Redis `XINFO GROUPS` drain checks when every live member expires before revoke acknowledgement.
* [x] Active migration rollback.
* [x] Monitoring/read API operational refresh retry on Redis store CAS conflict.

### Redis Integration

* [x] External Redis Cluster Docker Compose profiles for sample pods and stress smoke tests.
* [x] Redis health check in coordinator health response when Redis is required by active configuration.
* [x] `CoordinatorStateStore` abstraction.
* [x] In-memory state store.
* [x] Redis state store for aggregate group metadata.
* [x] Redis single metadata hash key per group.
* [x] Redis state mutex key for request-level coordinator critical-section serialization.
* [x] Redis store revision compare-and-set for stale write detection.
* [x] Redis metadata `schemaVersion` guard for persisted group aggregate reads and writes.
* [x] Lua metadata hash updates to avoid stale writer overwrites.
* [x] Redis Cluster hash-slot-safe coordinator keys.
* [x] Redis Stream shard key helper and hash-slot distribution helper.
* [x] Optional Redis Stream shard and consumer-group provisioning.
* [x] Redis Stream provisioning idempotent retry coverage after partial Redis failure.
* [x] Centralized coordinator Redis command template for state, mutex, audit, health, and stream provisioning commands.

### Redis Metadata Correction

* [x] One canonical Redis metadata key per `{streamPrefix, consumerGroup}`.
* [x] Coordinator detects heartbeats that report a higher local `metadataVersion` than the Redis metadata key.
* [x] Coordinator returns `SYNC_METADATA` so consumers discard the higher local view and use the current coordinator metadata.
* [x] `SYNC_METADATA` is retry-safe and drain-only; new shard reads are blocked until a later `OK`.
* [x] `REVOKE_PENDING` separates corrected metadata from unfinished revoke-before-assign handoff.
* [x] Metadata correction remains active until live members heartbeat with the target metadata version.
* [x] Stale revoke reports from discarded higher metadata views are ignored during correction.

### Security, Audit, And Observability

* [x] Basic Auth for admin and monitoring APIs.
* [x] Optional member heartbeat authentication.
* [x] Role ACL with `ADMIN`, `MONITOR`, and `MEMBER`.
* [x] Optional per-caller/group admin mutation rate limiting with `Retry-After` response.
* [x] Structured admin audit logs for create, delete, scale, and rollback.
* [x] Optional Redis-backed group-scoped admin audit log.
* [x] Built-in monitoring console that signs in with coordinator Basic Auth and reads monitoring APIs.
* [x] Coordinator Micrometer metrics:
  * `redis_stream_coord_up`
  * `redis_stream_coord_group_epoch`
  * `redis_stream_coord_assignment_epoch`
  * `redis_stream_coord_members`
  * `redis_stream_coord_member_active`
  * `redis_stream_coord_member_heartbeat_age_seconds`
  * `redis_stream_coord_member_lease_remaining_seconds`
  * `redis_stream_coord_member_runtime_max_concurrency`
  * `redis_stream_coord_member_active_workers`
  * `redis_stream_coord_member_current_shards`
  * `redis_stream_coord_member_revoking_shards`
  * `redis_stream_coord_heartbeat_total`
  * `redis_stream_coord_member_expired_total`
  * `redis_stream_coord_rebalance_total`
  * `redis_stream_coord_rebalance_duration`
  * `redis_stream_coord_scale_request_total`
  * `redis_stream_coord_scale_request_failed_total`
  * `redis_stream_coord_consumer_concurrency_update_total`
  * `redis_stream_coord_producer_routing_request_total`
  * `redis_stream_coord_migration_active`
  * `redis_stream_coord_migration_active_age_seconds`
  * `redis_stream_coord_revoke_pending`
  * `redis_stream_coord_invariant_violations`
  * `redis_stream_coord_invariant_violation_total`
  * `redis_stream_coord_tick_total`
  * `redis_stream_coord_tick_duration`
  * `redis_stream_coord_state_conflict_total`
  * `redis_stream_coord_consumer_shard_last_delivered_ms`
  * `redis_stream_coord_consumer_shard_last_delivered_seq`
  * `redis_stream_coord_consumer_shard_last_acked_ms`
  * `redis_stream_coord_consumer_shard_last_acked_seq`
  * `redis_stream_coord_consumer_shard_pending`
  * `redis_stream_coord_consumer_shard_progress_updated_at_seconds`
  * `redis_stream_coord_consumer_shard_progress_age_seconds`
  * `redis_stream_coord_shard_stream_length`
  * `redis_stream_coord_shard_pending`
  * `redis_stream_coord_shard_lag`
  * `redis_stream_coord_shard_last_record_ms`
  * `redis_stream_coord_shard_last_record_seq`
  * `redis_stream_coord_shard_last_generated_ms`
  * `redis_stream_coord_shard_last_generated_seq`
  * `redis_stream_coord_shard_group_last_delivered_ms`
  * `redis_stream_coord_shard_group_last_delivered_seq`
  * `redis_stream_coord_shard_consumer_last_acked_ms`
  * `redis_stream_coord_shard_consumer_last_acked_seq`
* [x] Local Prometheus/Grafana Docker provisioning:
  * Prometheus scrape config for `/actuator/prometheus`.
  * Grafana datasource provisioning.
  * Redis Stream Coordinator dashboard.
  * Coordinator API datasource with Grafana-managed Basic Auth.
  * Direct Grafana REST panels for group/member/assignment/shard/message monitoring APIs.
  * Produced/s and consumed/s monitoring in group, shard, and time-series panels.
  * Stream message payload table with cursor pagination through dashboard variables.

### RedisStream Spring Boot Starter

* [x] `CoordinatorClient` interface.
* [x] `RestClient`-based coordinator client.
* [x] `CoordinatorShardLifecycle` callback contract.
* [x] `CoordinatorManagedConsumer` heartbeat lifecycle.
* [x] Assignment, pending, revoke, fencing, rejoin, and graceful leave handling.
* [x] Repeated revoke callback support for long drain windows.
* [x] Preservation of earlier draining revoke reports when additional revokes occur.
* [x] Optional `CoordinatorRuntimeCapacityProvider` for application-reported runtime capacity.
* [x] Optional `CoordinatorShardProgressProvider` for coordinator-reported shard progress.
* [x] Built-in Redis Stream polling lifecycle reports in-flight handler capacity.
* [x] Built-in Redis Stream polling lifecycle reports last delivered and last acked Redis Stream ids.
* [x] Consumer-side coordination version validation.
* [x] Spring Boot auto-configuration.
* [x] Opt-in Redis Stream polling adapter.
* [x] Handler-success-only `XACK` behavior.
* [x] Producer routing metadata cache.
* [x] Producer routing metadata validation.
* [x] Producer routing cache invalidation after Redis write failure.
* [x] Opt-in bounded publisher retry with refreshed routing metadata.
* [x] Redis Stream publisher.
* [x] Convenience payload publish API.
* [x] Ordered best-effort batch publish API.
* [x] Shared Redis Stream command template for producer and consumer Redis commands.
* [x] Starter modules report progress to the coordinator instead of publishing separate Micrometer meters.

## Documented Constraints

* [x] Same partition key can route to a different Redis Stream shard after shard scale-out/in because routing is scoped to the shard count.
* [x] Producer retry and shard migration can create duplicate messages or duplicate business-event attempts.
* [x] Duplicate-sensitive workloads must quiesce producers and drain in-flight publish retries before shard scale-out/in.
* [x] Processing guarantee is at-least-once. Single delivery, single handler invocation, or single business side-effect application is not guaranteed.

### Docker, CI, And Open Source Docs

* [x] Coordinator server Dockerfile with Java 24 runtime and non-root user.
* [x] `compose.pods.yaml` for coordinator, sample pods, Prometheus, and Grafana against an external Redis Cluster.
* [x] `compose.stress.yaml` for external-Redis producer/consumer stress smoke.
* [x] Docker smoke workflow that builds the image and checks `/coord/v1/monitoring/health`.
* [x] Manual GHCR publish workflow for versioned coordinator image tags.
* [x] Docker distribution metadata test script.
* [x] Contributor guide.
* [x] Security policy.
* [x] Changelog.
* [x] Testing guide.
* [x] Operations runbook.

## Key Tests

### Coordinator

* Group creation and duplicate rejection.
* Heartbeat validation and unsupported protocol rejection.
* Heartbeat replay idempotency for initial join, steady ownership reports, and active epoch reset rejection after ownership is acknowledged.
* Member join, expired rejoin, graceful leave, and unknown member leave.
* Sticky assignment and weighted balancing.
* Revoke-before-assign handoff with `pendingShards`.
* Member expiration through heartbeat and event loop tick.
* Rebalance timeout fencing.
* Stale ownership fencing for premature pending ownership and foreign active-owner shard reports.
* Scale migration, producer routing refresh, rollback, migration drain completion, and no-live-member scale-in drain completion.
* Redis single-key metadata store and stale snapshot rejection.
* Stream shard key validation and Redis Cluster slot distribution.
* Stream provisioning success, idempotent retry, and failure ordering.
* ACL enforcement and admin audit events.
* Admin mutation rate limiting.
* Monitoring refresh conflict retry.
* Coordinator Micrometer metrics, including consumer shard progress gauges.

### Starter

* Join heartbeat and assignment callbacks.
* Removed assignment revoke reporting.
* Incomplete revoke retry and later `REVOKED` reporting.
* Pending shard callback without owned report.
* `RETRY` response retains owned state.
* Fencing resets local ownership and rejoins with `memberEpoch=0`.
* Graceful leave heartbeat on shutdown.
* Redis polling adapter reads, invokes handler, and acknowledges only successful messages.
* Runtime capacity reports available concurrency while handlers are in flight.
* Redis polling enforces runtime concurrency across assigned shards before reading new records.
* Shard progress reports last delivered and last acked Redis Stream ids through heartbeat.
* Producer routing cache refresh, invalidation, validation, and unsupported hash rejection.
* Redis Stream publisher routing, stale-cache invalidation, opt-in retry, payload helper, and batch publish.

### Docker And Docs

* Docker distribution metadata guard.

## File Map

| Area | Primary files |
| --- | --- |
| Domain model | `coordinator-server/src/main/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/domain/CoordinatorModels.kt` |
| Coordinator service | `coordinator-server/src/main/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/service/CoordinatorService.kt` |
| Event loop and metrics | `CoordinatorEventLoop.kt`, `CoordinatorMetrics.kt` |
| HTTP API and errors | `CoordinatorControllers.kt`, `CoordinatorErrors.kt` |
| Config, auth, audit | `CoordinatorProperties.kt`, `CoordinatorAuth.kt`, `CoordinatorAudit.kt`, `RedisClientConfig.kt` |
| State store | `CoordinatorStateStore.kt` |
| Coordinator Redis commands | `coordinator-server/src/main/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/redis/CoordinatorRedisCommands.kt` |
| Stream provisioning | `RedisStreamProvisioning.kt`, `RedisStreamShardKeys.kt` |
| Consumer starter | `redisstream-spring-boot-starter/src/main/kotlin/com/redisstream/consumer/*` |
| Producer starter | `redisstream-spring-boot-starter/src/main/kotlin/com/redisstream/producer/*` |
| Docker distribution | `Dockerfile`, `.dockerignore`, `compose.pods.yaml`, `compose.stress.yaml`, `.github/workflows/docker-image.yml`, `docs/docker.md` |
| Open source docs | `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`, `docs/testing.md`, `docs/operations-runbook.md` |
| PRD | `docs/PRD.md`, `docs/prd/*` |

## Remaining Work

Priority order:

1. [ ] Cut the first public Docker image release through the manual GHCR workflow.
2. [ ] Add a compatibility fixture suite for old metadata JSON and old client coordination versions.

Still intentionally out of coordinator scope:

* Redis Stream data-plane reads.
* `XACK`.
* Handler execution.
* Retry and DLQ policy.
* Idempotency marker lifecycle.
