# 구현 상태

마지막 업데이트: 2026-05-29

## 요약

현재 저장소는 두 개의 주요 Gradle 모듈을 제공한다.

| Module | Status | Purpose |
| --- | --- | --- |
| `coordinator-server` | MVP 구현 | group metadata, heartbeat reconciliation, assignment, resharding, monitoring, ACL, audit, Redis-backed state를 담당하는 Spring Boot control plane |
| `com.redisstream:redisstream-spring-boot-starter` | MVP 구현 | consumer heartbeat lifecycle, shard callback, Redis Stream polling, producer routing, publish, graceful leave, progress report를 제공하는 Spring Boot integration layer |

## 구현 완료

### Coordinator Server

* [x] Group 생성 API
* [x] Group metadata 조회 API
* [x] Producer routing metadata API
* [x] Shard scale API
* [x] Consumer concurrency update API
* [x] Resharding 조회와 rollback API
* [x] Member heartbeat API
* [x] Health, group, member, assignment, resharding monitoring API
* [x] HTTP status, error code, default message를 enum으로 관리하는 error model
* [x] Member lease expiry, rebalance timeout, drain progress를 처리하는 scheduled event loop
* [x] 여러 coordinator pod가 떠도 state 접근을 직렬화할 수 있는 Redis-backed state mutex

### Coordination Semantics

* [x] `groupEpoch`, `metadataVersion`, `assignmentEpoch` 기반 group metadata model
* [x] Heartbeat protocol compatibility range
* [x] Member epoch validation
* [x] `memberEpoch=0` 기반 join/rejoin
* [x] Graceful leave
* [x] Member lease expiry와 fencing
* [x] Unauthorized `ownedShards` / invalid `revokingShards` 보고에 대한 stale ownership fencing
* [x] Server-side consumer concurrency policy를 반영한 sticky assignment
* [x] `assignedShards`와 `pendingShards` 분리
* [x] Revoke-before-assign handoff
* [x] Rebalance timeout fencing
* [x] Next stream version 기반 shard count migration
* [x] Active migration 중 old/new readable versions
* [x] Drain 완료 후 old version `DEPRECATED` 전환
* [x] Active resharding rollback

### Redis Integration

* [x] Local three-node Redis Cluster Docker Compose
* [x] Host-to-Docker Redis Cluster redirect를 위한 Lettuce node address mapping
* [x] Memory state store
* [x] Redis state store
* [x] Redis group별 단일 metadata hash key
* [x] Redis state mutex key
* [x] Redis store revision compare-and-set
* [x] Redis metadata `schemaVersion` guard
* [x] Redis group별 단일 metadata hash key
* [x] Lua metadata hash update
* [x] Redis Cluster hash-slot-safe coordinator keys
* [x] Optional Redis Stream shard and consumer-group provisioning
* [x] Coordinator Redis command template

### Redis Metadata Correction

* [x] `{streamPrefix, consumerGroup}`당 하나의 canonical Redis metadata key 사용
* [x] Consumer heartbeat가 Redis metadata보다 높은 `metadataVersion`을 보고하면 coordinator가 감지
* [x] Coordinator가 `SYNC_METADATA` 응답으로 consumer의 local metadata view를 현재 Redis metadata로 수정 요청
* [x] `SYNC_METADATA`는 retry-safe drain-only 응답이며, 신규 shard read는 이후 `OK`까지 차단
* [x] `REVOKE_PENDING`으로 metadata correction 완료 전 revoke-before-assign 대기 상태 분리
* [x] Live member들이 target metadata version으로 heartbeat할 때까지 correction round 유지
* [x] 폐기된 상위 metadata view에서 온 stale revoke report 무시

### Security, Audit, Observability

* [x] Basic Auth for admin and monitoring APIs
* [x] Optional member heartbeat authentication
* [x] Role ACL: `ADMIN`, `MONITOR`, `MEMBER`
* [x] Per-caller/group admin mutation rate limiting
* [x] Structured admin audit logs
* [x] Optional Redis-backed group-scoped audit log
* [x] Coordinator-owned Micrometer metrics
* [x] Consumer shard progress gauges

### Spring Boot Starter

* [x] `CoordinatorClient`
* [x] RestClient 기반 coordinator client
* [x] `CoordinatorShardLifecycle`
* [x] `CoordinatorManagedConsumer`
* [x] Assignment, pending, revoke, fencing, rejoin, graceful leave 처리
* [x] Runtime capacity provider
* [x] Shard progress provider
* [x] Redis Stream polling adapter
* [x] Handler success 이후 ACK
* [x] Producer routing metadata cache
* [x] Stale routing cache invalidation
* [x] Bounded publish retry
* [x] Redis Stream publisher
* [x] Payload helper and batch publish
* [x] Shared Redis Stream command template

## 문서화된 제약

* [x] 동일 partition key도 shard scale-out/in 이후 다른 shard로 route될 수 있다.
* [x] Producer retry와 resharding은 duplicate message 또는 duplicate business-event attempt를 만들 수 있다.
* [x] Duplicate-sensitive workload는 scale 전 producer를 멈추고 in-flight publish retry를 drain해야 한다.
* [x] 기본 처리 보장은 at-least-once이다.
* [x] Single delivery, single handler invocation, single business side-effect application은 보장하지 않는다.

## 검증 명령

```bash
./gradlew :coordinator-server:test --no-daemon
./gradlew :redisstream-spring-boot-starter:test --no-daemon
./gradlew test build --no-daemon
python3 .github/scripts/test_docker_distribution.py
```

Redis integration tests:

```bash
docker compose up -d
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test \
  --tests '*RedisCoordinatorStateStoreIntegrationTest' \
  --tests '*RedisStreamProvisioningIntegrationTest'
```

## 남은 작업

1. [ ] 첫 public Docker image release
2. [ ] 외부 배포 예제와 release automation hardening
3. [ ] Redis version compatibility guide 보강
4. [ ] 오래된 metadata JSON과 client coordination version compatibility fixture 보강
5. [ ] 운영 dashboard/alert 예제 보강
