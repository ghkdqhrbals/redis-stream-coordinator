# Implementation Status

Last updated: 2026-05-22

## Summary

The repository now has a Spring Boot 4 / Kotlin / Java 24 Gradle module named `coordinator-server`.

The current implementation is an MVP control-plane server for the Redis Stream Coordinator PRD. It exposes the planned HTTP API surface, manages group metadata, member heartbeat state, target/current assignment, migration state, and basic monitoring responses.

The coordinator state defaults to memory, and group-level metadata can also be stored in Redis by setting `coordinator.store.type=redis`.
The module is connected to a local three-node Redis Cluster for connectivity, metadata-store work, and Redis Stream shard provisioning tests.
Stream shard key formatting, Redis Cluster hash-slot planning, and opt-in Redis Stream shard provisioning are now implemented.

## Build and Tooling

Implemented:

* Gradle Kotlin DSL multi-module project.
* Gradle Wrapper `8.14.5`.
* Spring Boot `4.0.6`.
* Kotlin `2.2.21`.
* Java toolchain `24`.
* Foojay toolchain resolver so Gradle can provision Java 24 when it is not installed locally.
* Version catalog for project plugin versions.
* `.gitignore` for Gradle, IDE, build, and swap files.
* IntelliJ setup guide: [`docs/intellij-setup.md`](intellij-setup.md).
* Docker Compose Redis Cluster with three master nodes.

Verified:

```bash
./gradlew :coordinator-server:compileJava
./gradlew :coordinator-server:test
./gradlew :coordinator-server:build
```

All passed.

Local note: Gradle/Kotlin DSL currently fails when launched directly on Java 25, so the local test command uses JDK 17 as the Gradle launcher while the project still compiles with the configured Java 24 toolchain:

```bash
env JAVA_HOME=/Users/gyuminhwangbo/Library/Java/JavaVirtualMachines/corretto-17.0.10/Contents/Home ./gradlew :coordinator-server:test
```

Redis integration tests are gated so the default suite does not require Docker:

```bash
docker compose up -d
REDIS_COORDINATOR_INTEGRATION_TESTS=true \
  env JAVA_HOME=/Users/gyuminhwangbo/Library/Java/JavaVirtualMachines/corretto-17.0.10/Contents/Home \
  ./gradlew :coordinator-server:test --tests io.github.ghkdqhrbals.redisstreamcoordinator.RedisCoordinatorStateStoreIntegrationTest
```

Provisioning-focused Redis integration tests can be run with:

```bash
REDIS_COORDINATOR_INTEGRATION_TESTS=true \
  ./gradlew :coordinator-server:test --tests io.github.ghkdqhrbals.redisstreamcoordinator.RedisStreamProvisioningIntegrationTest
```

On pull requests, the `PR test results` workflow runs PR code in a read-only test job, uploads the Gradle test report as the `coordinator-gradle-test-report` artifact, publishes a grouped HTML test report with expandable scenario sections, and updates a PR comment from a separate job that only has comment-related write permissions.

## Implemented Module

Module:

```text
coordinator-server
```

Main files:

* `CoordinatorModels.kt`
* `CoordinatorService.kt`
* `CoordinatorControllers.kt`
* `CoordinatorAuth.kt`
* `CoordinatorErrors.kt`
* `RedisStreamCoordinatorApplication.kt`
* `application.yaml`
* `CoordinatorStateStore.kt`
* root `compose.yaml`

## Implemented API Surface

Base path:

```text
/coord/v1
```

Implemented admin APIs:

* `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}`
* `GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}`
* `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/scale`
* `PATCH /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/consumer-concurrency`
* `GET /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{migrationId}`
* `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/migrations/{migrationId}/rollback`

Implemented member API:

* `POST /coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat`

Implemented monitoring APIs:

* `GET /coord/v1/monitoring/health`
* `GET /coord/v1/monitoring/groups`
* `GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}`
* `GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/members`
* `GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/assignments`
* `GET /coord/v1/monitoring/streams/{streamPrefix}/groups/{consumerGroup}/migrations`

## Implemented Behavior

Implemented:

* Group creation with initial stream version `1`.
* Server-side consumer concurrency policy.
* Member join/rejoin through heartbeat with `memberEpoch=0`.
* Graceful leave through heartbeat with `memberEpoch=-1`.
* Member lease expiration based on `member-lease-ttl`.
* Group epoch, metadata version, and assignment epoch tracking.
* Sticky assignment with minimal movement and balancing across live members.
* `assignedShards` and `pendingShards` separation.
* Revoke-before-assign rule: a shard is not assigned to a new owner while another active/leaving member still reports ownership or revocation.
* Scale request that creates a next stream version and keeps old/new versions readable.
* Rollback for active migration.
* Monitoring summaries for members, assignments, migrations, and invariant violations.
* Basic Auth for admin and monitoring APIs.
* Optional Basic Auth for member heartbeat API via config.
* Spring Data Redis dependency and Redis Cluster connectivity health check.
* Lettuce node address mapping for local Docker Redis Cluster access from the host JVM.
* `CoordinatorStateStore` abstraction with memory and Redis implementations.
* Redis-backed group metadata persistence when `coordinator.store.type=redis`.
* Redis projected keys for member metadata, target assignments, current assignments, active migration, and migration history.
* Redis group-scoped aggregate/projection keys are replaced through one Lua script to avoid reader-visible partial projection updates.
* Redis-backed saves use a coordinator `storeRevision` compare-and-set guard and retry mutation requests on transient state conflicts.
* Redis Stream shard key helper with Redis Cluster hash-slot calculation and equal-master-range distribution planning.
* Opt-in Redis Stream shard provisioning through `coordinator.streams.provisioning-enabled=true`.
* Redis Stream consumer group provisioning with idempotent `XGROUP CREATE ... MKSTREAM` semantics during group creation and scale.

## Verified Runtime Smoke Test

The server was started with:

```bash
./gradlew :coordinator-server:bootRun
```

Health endpoint returned:

```json
{"status":"UP","coordinatorId":"local-coordinator","redis":"UP","loop":"UP"}
```

Group creation and first member heartbeat were also verified locally.

Default local credentials:

```text
admin:password
```

## Redis Cluster

Status:

* [x] `compose.yaml` starts three Redis 7.4 cluster nodes.
* [x] `redis-cluster-init` initializes the cluster with three masters and no replicas.
* [x] Host ports are exposed:
  * `localhost:7001`
  * `localhost:7002`
  * `localhost:7003`
* [x] All 16384 hash slots are assigned across the three nodes.
* [x] Spring Boot module connects to the cluster through Lettuce.
* [x] Coordinator group metadata can be persisted to Redis keys when `coordinator.store.type=redis`.

Verified:

```bash
docker compose up -d
docker compose exec -T redis-node-1 redis-cli cluster info
```

Cluster state:

```text
cluster_state:ok
cluster_slots_assigned:16384
cluster_known_nodes:3
cluster_size:3
```

Spring configuration:

```yaml
spring:
  data:
    redis:
      cluster:
        nodes:
          - localhost:7001
          - localhost:7002
          - localhost:7003
```

Because `CLUSTER NODES` still contains Docker-internal node addresses, the application also configures Lettuce address mapping by advertised port:

```text
*:7001 -> 127.0.0.1:7001
*:7002 -> 127.0.0.1:7002
*:7003 -> 127.0.0.1:7003
```

The Redis nodes use `cluster-announce-hostname 127.0.0.1` and `cluster-preferred-endpoint-type hostname`. This makes raw `MOVED` replies return a host-reachable endpoint, for example:

```text
MOVED 7486 127.0.0.1:7002
```

That form is important for host tools because a cluster-aware client connected to `127.0.0.1:7001` can follow the redirect to `127.0.0.1:7002` instead of trying to connect to a Docker-internal IP like `172.20.x.x`.

### How to Connect to the Cluster

Start the cluster:

```bash
docker compose up -d
```

Check container status:

```bash
docker compose ps
```

Connect from the host using `redis-cli`:

```bash
redis-cli -c -h 127.0.0.1 -p 7001
```

The `-c` option is required so `redis-cli` follows cluster redirects.

If `-c` is omitted, a key that belongs to another slot returns a redirect:

```text
MOVED 7486 127.0.0.1:7002
```

That is expected.

Example commands:

```redis
CLUSTER INFO
CLUSTER NODES
SET hello world
GET hello
```

Connect through Docker if local `redis-cli` is not installed:

```bash
docker compose exec -T redis-node-1 redis-cli -c -p 6379
```

Run one-shot commands:

```bash
docker compose exec -T redis-node-1 redis-cli -c -p 6379 cluster info
docker compose exec -T redis-node-1 redis-cli -c -p 6379 cluster nodes
docker compose exec -T redis-node-1 redis-cli -c -p 6379 set hello world
docker compose exec -T redis-node-1 redis-cli -c -p 6379 get hello
```

Stop the cluster:

```bash
docker compose down
```

Stop the cluster and remove persisted Redis data:

```bash
docker compose down -v
```

Application connection settings:

```yaml
spring:
  data:
    redis:
      cluster:
        nodes:
          - localhost:7001
          - localhost:7002
          - localhost:7003
```

Health check after starting the coordinator server:

```bash
curl -u admin:password http://127.0.0.1:8080/coord/v1/monitoring/health
```

Expected response:

```json
{"status":"UP","coordinatorId":"local-coordinator","redis":"UP","loop":"UP"}
```

## Implementation Plan

### Phase 1: Project and Runtime Foundation

* [x] Create Gradle multi-module project.
* [x] Add Gradle Wrapper `8.14.5`.
* [x] Configure Spring Boot `4.0.6`.
* [x] Configure Kotlin `2.2.21`.
* [x] Configure Java `24` toolchain.
* [x] Add Foojay Java toolchain resolver.
* [x] Add IntelliJ setup guide.
* [x] Add `.gitignore`.

### Phase 2: Coordinator API Surface

* [x] Add admin group creation API.
* [x] Add admin group metadata read API.
* [x] Add shard scale API.
* [x] Add consumer concurrency update API.
* [x] Add migration lookup API.
* [x] Add migration rollback API.
* [x] Add member heartbeat API.
* [x] Add monitoring health API.
* [x] Add monitoring group list API.
* [x] Add monitoring member list API.
* [x] Add monitoring assignment API.
* [x] Add monitoring migration API.
* [x] Add validation error responses.
* [x] Add Basic Auth for admin and monitoring APIs.
* [x] Make member heartbeat authentication configurable.

### Phase 3: In-Memory Coordination Semantics

* [x] Create group metadata model.
* [x] Track `groupEpoch`.
* [x] Track `metadataVersion`.
* [x] Track `assignmentEpoch`.
* [x] Track active write version and readable versions.
* [x] Track member metadata and lease expiry.
* [x] Support member join/rejoin with `memberEpoch=0`.
* [x] Support graceful leave with `memberEpoch=-1`.
* [x] Expire members after `member-lease-ttl`.
* [x] Compute sticky assignment.
* [x] Balance shards across live members.
* [x] Separate `assignedShards` and `pendingShards`.
* [x] Enforce revoke-before-assign.
* [x] Create next stream version on scale.
* [x] Keep old and new versions readable during migration.
* [x] Support active migration rollback.
* [x] Advance assignment epoch for capacity-policy-driven rebalances.
* [ ] Complete stricter stale member fencing semantics.
* [x] Add rebalance timeout handling.
* [ ] Add automatic migration drain completion and `DEPRECATED` transition.

### Phase 4: Redis Cluster Environment

* [x] Add Docker Compose for three Redis nodes.
* [x] Add cluster init container.
* [x] Verify all 16384 slots are assigned.
* [x] Add Spring Redis Cluster configuration.
* [x] Add Lettuce address mapping for host-to-Docker cluster access.
* [x] Add Redis health check in coordinator health endpoint.
* [x] Add gated Redis integration tests.

### Phase 5: Redis-Backed Coordinator Store

* [x] Introduce `CoordinatorStateStore` abstraction.
* [x] Move current in-memory state behind `InMemoryCoordinatorStateStore`.
* [x] Implement Redis group metadata key.
* [x] Implement Redis member metadata keys.
* [x] Implement Redis target assignment key.
* [x] Implement Redis current assignment keys.
* [x] Implement Redis active migration key.
* [x] Implement Redis migration history keys.
* [x] Implement Redis store revision key for stale write detection.
* [ ] Implement Redis admin audit log.
* [x] Add optimistic concurrency or Lua transaction boundaries for coordinator mutations.
* [x] Add store-level tests.

### Phase 6: Redis Stream Shard Operations

* [x] Create stream shard keys during group creation when stream provisioning is enabled.
* [x] Create Redis consumer groups for each shard when stream provisioning is enabled.
* [x] Create next-version shard keys during scale when stream provisioning is enabled.
* [x] Create consumer groups for next-version shards when stream provisioning is enabled.
* [x] Validate stream version and shard count metadata before building shard keys.
* [x] Add shard key format helper.
* [x] Add hash-slot distribution helper.

### Phase 7: Observability and Operations

* [ ] Add structured audit logs.
* [ ] Add Micrometer metrics from the PRD.
* [ ] Add invariant violation metric.
* [ ] Add active migration age metric.
* [ ] Add member expiration metric.
* [ ] Add rate limiting for admin APIs.
* [ ] Add operational runbook.

## Tests Added

Implemented tests:

* First heartbeat assigns all readable shards to the first member.
* New member receives moved shard as `pendingShards` until previous owner reports revoke.
* Scale request creates next stream version and exposes old/new readable versions.
* Duplicate group creation is rejected.
* Invalid heartbeat path/body member mismatch is rejected.
* Missing group heartbeat is rejected as `UNKNOWN_MEMBER_ID`.
* Unknown member cannot leave by sending `memberEpoch=-1`.
* Expired member is removed from target assignment and shards are reassigned after lease expiry.
* Consumer concurrency policy changes rebalance target assignments by member weight.
* Consumer concurrency policy changes that move assignments advance `groupEpoch`, `assignmentEpoch`, and subsequent heartbeat `memberEpoch`.
* Rebalance timeout fences an old owner that does not revoke a moved shard and keeps a timely revoker active.
* Category-level grouped workflows cover member expiration, member join, graceful leave, partition upscaling, and new stream topic creation.
* Rollback restores previous stream version and rejects unknown migration IDs.
* Expired member can rejoin with `memberEpoch=0`.
* 432 focused Kafka-style operational matrix scenarios cover shard counts, member counts, scale up/down/rollback, steady/add/leave/expire/restart/replace churn, and uniform/skewed member capacity with descriptive scenario names.
* In-memory state store supports create/get/save/list.
* Coordinator state survives service instance replacement when the same state store is reused.
* Redis state projection splits aggregate state into member, target, current assignment, migration, and active migration sections.
* Redis key helper keeps group-scoped keys in a single Redis Cluster hash slot and preserves configured prefix formatting.
* Redis-backed store rejects stale coordinator snapshots instead of overwriting newer state.
* Redis Stream shard key helper rejects hash-tag unsafe stream prefixes, validates version/shard counts, calculates Redis Cluster slots, and estimates distribution across equal master ranges.
* Coordinator service calls shard provisioning on group creation and scale.
* Gated Redis integration verifies provisioned Redis Stream consumer groups for initial and next-version shards.
* Gated Redis integration verifies direct stream provisioning is idempotent when Redis consumer groups already exist.
* HTTP integration covers Basic Auth, request validation, group creation, member heartbeat, and monitoring assignments.
* Gated Redis integration verifies aggregate and projected PRD keys against a local Redis Cluster.
* Spring application context loads.

Test files:

```text
coordinator-server/src/test/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/CoordinatorServiceTest.kt
coordinator-server/src/test/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/CoordinatorGroupedWorkflowTest.kt
coordinator-server/src/test/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/CoordinatorOperationalScenarioMatrixTest.kt
coordinator-server/src/test/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/CoordinatorStateStoreTest.kt
coordinator-server/src/test/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/CoordinatorHttpIntegrationTest.kt
coordinator-server/src/test/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/RedisCoordinatorStateStoreIntegrationTest.kt
coordinator-server/src/test/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/RedisStreamProvisioningIntegrationTest.kt
```

## Not Implemented Yet

Remaining work:

* Redis admin audit log.
* Coordinator event loop as a scheduled worker.
* Admin audit logging.
* Metrics listed in the PRD.
* Rate limiting.
* Full authorization model beyond Basic Auth.
* Migration drain progress and automatic `DEPRECATED` transition.
* Producer routing metadata API/cache.
* More complete epoch fencing semantics.
* Broader Redis integration tests that stress idempotent provisioning retry and failure handling.

Explicitly still out of scope for the coordinator:

* Redis Stream message read.
* `XACK`.
* Retry/DLQ.
* Handler execution.
* Idempotency marker management.

## Suggested Next Step

Next implementation step should be to expose producer routing metadata and finish migration drain semantics:

1. Add a producer routing metadata API/cache that returns active write version, shard count, key pattern, hash algorithm, and hash seed.
2. Add metadata-version based cache invalidation for producer clients.
3. Detect old-version drain completion and transition active migrations to `DEPRECATED`.
4. Add retry/failure integration tests for stream provisioning.
