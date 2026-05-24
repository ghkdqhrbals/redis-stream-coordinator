# Implementation Status

Last updated: 2026-05-24

## Snapshot

The repository currently contains two Gradle modules:

| Module | Status | Purpose |
| --- | --- | --- |
| `coordinator-server` | MVP implemented | Spring Boot control plane for group metadata, heartbeat reconciliation, assignment, migration, monitoring, ACL, audit, metrics, and Redis-backed state. |
| `com.redisstream:redisstream-spring-boot-starter` | MVP implemented | Spring Boot integration layer for consumer heartbeat lifecycle, shard callbacks, Redis Stream polling, producer routing, publishing, graceful leave, and metrics. |

Overall status:

| Area | Status | Notes |
| --- | --- | --- |
| Project foundation | Done | Gradle Kotlin DSL, Gradle Wrapper `8.14.5`, Spring Boot `4.0.6`, Kotlin `2.2.21`, Java toolchain `24`, Foojay resolver. |
| Coordinator API | Done | Admin, member heartbeat, producer routing, migration, rollback, and monitoring endpoints are implemented. |
| Rebalance semantics | Mostly done | Sticky assignment, revoke-before-assign, member join/rejoin/leave/expiry, rebalance timeout, and migration drain are implemented. Stricter stale member fencing remains. |
| Redis state store | Done | Memory and Redis stores are available. Redis writes use store revision compare-and-set, schema version guard, and Lua aggregate/projection updates. |
| Redis Stream shard provisioning | Done | Optional initial and next-version stream/consumer-group provisioning is implemented and gated by config. |
| Security and audit | Done for MVP | Basic Auth, role ACL, structured audit logs, optional Redis audit sink, and per-caller/group admin mutation rate limiting are implemented. |
| Observability | Done for MVP | Coordinator and starter Micrometer metrics are implemented. Monitoring APIs are implemented. |
| Consumer starter | Done for MVP | Heartbeat lifecycle, shard callbacks, fencing/rejoin, pending/revoking handling, graceful leave, and opt-in Redis polling adapter are implemented. |
| Producer starter | Done for MVP | Producer routing cache, routing validation, Redis Stream publisher, payload helper, batch publish, and metrics are implemented. |
| Local Redis Cluster | Done | `compose.yaml` starts three Redis Cluster masters and supports host access through `localhost:7001..7003`. |
| Docker distribution | Ready for MVP | Dockerfile, local Compose coordinator profile, PR smoke test, manual GHCR publish workflow, and user guide are implemented. First public image release remains. |
| Open source operations | Ready for MVP | Contributing guide, security policy, changelog, testing guide, Docker guide, and operations runbook are available. |

## Verified Commands

The current implementation has been verified with:

```bash
./gradlew :coordinator-server:test --tests '*CoordinatorMetricsTest' --tests '*CoordinatorServiceTest' --no-daemon
./gradlew :redisstream-spring-boot-starter:test --no-daemon
./gradlew test build --no-daemon
python3 .github/scripts/test_docker_distribution.py
```

Redis integration tests are gated and require the local Redis Cluster:

```bash
docker compose up -d
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test --tests '*RedisCoordinatorStateStoreIntegrationTest'
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test --tests '*RedisStreamProvisioningIntegrationTest'
```

## Implemented

### Coordinator Server

* [x] Admin group creation API.
* [x] Group metadata read API.
* [x] Producer routing metadata API.
* [x] Shard scale API.
* [x] Consumer concurrency update API.
* [x] Migration lookup API.
* [x] Migration rollback API.
* [x] Member heartbeat API.
* [x] Monitoring health, group, member, assignment, and migration APIs.
* [x] Shared error enum for HTTP status, error code, and default message.
* [x] Scheduled coordinator event loop for lease expiry, rebalance timeout, and migration drain progress.

### Coordination Semantics

* [x] Group metadata model with `groupEpoch`, `metadataVersion`, and `assignmentEpoch`.
* [x] Heartbeat protocol compatibility range.
* [x] Coordinator-issued member epoch validation.
* [x] Join/rejoin with `memberEpoch=0`.
* [x] Graceful leave with `memberEpoch=-1`.
* [x] Member lease expiry and fencing state.
* [x] Sticky assignment with balancing by server-side consumer concurrency policy.
* [x] `assignedShards` and `pendingShards` split.
* [x] Revoke-before-assign handoff.
* [x] Rebalance timeout fencing.
* [x] Scale migration with next stream version.
* [x] Old/new readable versions during active migration.
* [x] Automatic migration drain and `DEPRECATED` transition.
* [x] Active migration rollback.

### Redis Integration

* [x] Three-node local Redis Cluster Docker Compose.
* [x] Lettuce node address mapping for host-to-Docker cluster redirects.
* [x] Redis health check in coordinator health response.
* [x] `CoordinatorStateStore` abstraction.
* [x] In-memory state store.
* [x] Redis state store for aggregate group metadata.
* [x] Redis projection keys for members, target assignment, current assignment, migrations, active migration, and revision.
* [x] Redis store revision compare-and-set for stale write detection.
* [x] Redis metadata `schemaVersion` guard for persisted group aggregate reads and writes.
* [x] Lua aggregate/projection updates to avoid reader-visible partial writes.
* [x] Redis Cluster hash-slot-safe coordinator keys.
* [x] Redis Stream shard key helper and hash-slot distribution helper.
* [x] Optional Redis Stream shard and consumer-group provisioning.

### Security, Audit, And Observability

* [x] Basic Auth for admin and monitoring APIs.
* [x] Optional member heartbeat authentication.
* [x] Role ACL with `ADMIN`, `MONITOR`, and `MEMBER`.
* [x] Optional per-caller/group admin mutation rate limiting with `Retry-After` response.
* [x] Structured admin audit logs for create, scale, consumer concurrency update, and rollback.
* [x] Optional Redis-backed group-scoped admin audit log.
* [x] Coordinator Micrometer metrics:
  * `redis_stream_coord_up`
  * `redis_stream_coord_group_epoch`
  * `redis_stream_coord_assignment_epoch`
  * `redis_stream_coord_members`
  * `redis_stream_coord_heartbeat_total`
  * `redis_stream_coord_member_expired_total`
  * `redis_stream_coord_rebalance_total`
  * `redis_stream_coord_rebalance_duration`
  * `redis_stream_coord_scale_request_total`
  * `redis_stream_coord_scale_request_failed_total`
  * `redis_stream_coord_consumer_concurrency_update_total`
  * `redis_stream_coord_migration_active`
  * `redis_stream_coord_migration_active_age_seconds`
  * `redis_stream_coord_revoke_pending`
  * `redis_stream_coord_invariant_violations`
  * `redis_stream_coord_invariant_violation_total`
  * `redis_stream_coord_tick_total`
  * `redis_stream_coord_tick_duration`

### RedisStream Spring Boot Starter

* [x] `CoordinatorClient` interface.
* [x] `RestClient`-based coordinator client.
* [x] `CoordinatorShardLifecycle` callback contract.
* [x] `CoordinatorManagedConsumer` heartbeat lifecycle.
* [x] Assignment, pending, revoke, fencing, rejoin, and graceful leave handling.
* [x] Repeated revoke callback support for long drain windows.
* [x] Preservation of earlier draining revoke reports when additional revokes occur.
* [x] Consumer-side heartbeat protocol validation.
* [x] Spring Boot auto-configuration.
* [x] Opt-in Redis Stream polling adapter.
* [x] Handler-success-only `XACK` behavior.
* [x] Producer routing metadata cache.
* [x] Producer routing metadata validation.
* [x] Redis Stream publisher.
* [x] Convenience payload publish API.
* [x] Ordered best-effort batch publish API.
* [x] Consumer and producer Micrometer metrics.

### Docker, CI, And Open Source Docs

* [x] Coordinator server Dockerfile with Java 24 runtime and non-root user.
* [x] Local `docker compose --profile coordinator` path for Redis Cluster plus coordinator.
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
* Member join, expired rejoin, graceful leave, and unknown member leave.
* Sticky assignment and weighted balancing.
* Revoke-before-assign handoff with `pendingShards`.
* Member expiration through heartbeat and event loop tick.
* Rebalance timeout fencing.
* Scale migration, producer routing refresh, rollback, and migration drain completion.
* Redis state projection and stale snapshot rejection.
* Stream shard key validation and Redis Cluster slot distribution.
* Stream provisioning success and failure ordering.
* ACL enforcement and admin audit events.
* Admin mutation rate limiting.
* Coordinator Micrometer metrics.

### Starter

* Join heartbeat and assignment callbacks.
* Removed assignment revoke reporting.
* Incomplete revoke retry and later `REVOKED` reporting.
* Pending shard callback without owned report.
* `RETRY` response retains owned state.
* Fencing resets local ownership and rejoins with `memberEpoch=0`.
* Graceful leave heartbeat on shutdown.
* Redis polling adapter reads, invokes handler, and acknowledges only successful messages.
* Producer routing cache refresh, invalidation, validation, and unsupported hash rejection.
* Redis Stream publisher routing, payload helper, batch publish, and metrics.

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
| Stream provisioning | `RedisStreamProvisioning.kt`, `RedisStreamShardKeys.kt` |
| Consumer starter | `redisstream-spring-boot-starter/src/main/kotlin/com/redisstream/consumer/*` |
| Producer starter | `redisstream-spring-boot-starter/src/main/kotlin/com/redisstream/producer/*` |
| Redis Cluster | `compose.yaml` |
| Docker distribution | `Dockerfile`, `.dockerignore`, `.github/workflows/docker-image.yml`, `docs/docker.md` |
| Open source docs | `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`, `docs/testing.md`, `docs/operations-runbook.md` |
| PRD | `docs/PRD.md`, `docs/prd/*` |

## Remaining Work

Priority order:

1. [ ] Cut the first public Docker image release through the manual GHCR workflow.
2. [ ] Tighten stale member fencing semantics beyond the current coordinator-issued epoch checks.
3. [ ] Add broader Redis integration tests for idempotent provisioning retry and failure handling.

Still intentionally out of coordinator scope:

* Redis Stream data-plane reads.
* `XACK`.
* Handler execution.
* Retry and DLQ policy.
* Idempotency marker lifecycle.
