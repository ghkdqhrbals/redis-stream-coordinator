# Coordinator Data, Configuration, and Observability

## Scope

Redis Stream Coordinator is a rebalance control plane. It manages group metadata, member liveness, target/current assignment, routing metadata, resharding state, audit records, and monitoring projections.

It does not own data-plane processing state such as handler retry counters, business idempotency records, local caches, or application DLQ state.

## Redis Key Layout

Coordinator-owned metadata uses Redis Cluster-safe hash tags so the aggregate keys for one group can live in the same hash slot.

Example key pattern:

| Key | Purpose |
| --- | --- |
| `redis-stream:coord:{group}:state` | group aggregate metadata |
| `redis-stream:coord:{group}:projection` | monitoring projection |
| `redis-stream:coord:{group}:audit` | admin mutation audit events |
| `redis-stream:coord:{group}:mutex` | state mutex |
| `redis-stream:coord:groups` | global group index |

`redis-stream:coord:groups` is an index used to discover configured groups. Group-scoped metadata remains under hash-tagged group keys.

## Configuration Boundary

Coordinator YAML should contain infrastructure and operational defaults only:

* HTTP API path and security,
* Redis connection,
* state store mode,
* mutex behavior,
* event-loop interval,
* member lease timeout,
* default shard count and consumer concurrency used when Admin API requests omit values.

Coordinator YAML should not contain:

* a fixed `streamPrefix`,
* a fixed `consumerGroup`,
* a per-group shard count source of truth,
* producer routing cache policy for applications,
* member runtime worker tuning beyond coordinator defaults.

Per-group settings are created or changed through the Admin API.

## Example Configuration

```yaml
coordinator:
  api:
    base-path: /coord/v1
    admin-username: admin
    admin-password: ${COORDINATOR_ADMIN_PASSWORD:password}
    authenticate-member-api: false
    rate-limit:
      enabled: true
      window: 1m
      max-requests: 60
  audit:
    sink: redis
  redis:
    host: ${REDIS_HOST:127.0.0.1}
    port: ${REDIS_PORT:6379}
    database: ${REDIS_DATABASE:0}
    username: ${REDIS_USERNAME:}
    password: ${REDIS_PASSWORD:}
    tls: ${REDIS_TLS:false}
  coordination:
    heartbeat-interval: 1s
    member-lease-ttl: 10s
    event-loop:
      enabled: true
      interval: 1s
    state-mutex:
      enabled: true
      lease-ttl: 5s
      wait-timeout: 2s
      retry-interval: 100ms
  defaults:
    initial-shard-count: 4
    max-concurrency: 4
```

## State Mutex

For Redis-backed state, the state mutex should be enabled by default.

Protected operations:

* create group,
* heartbeat,
* graceful leave,
* resharding request,
* rollback,
* consumer concurrency update,
* monitoring read that performs operational refresh,
* scheduled event loop tick.

Critical section order:

```text
acquire mutex
  -> read latest Redis state
  -> validate/process/reconcile
  -> save with storeRevision compare-and-set
  -> release mutex
```

The mutex is not intended to implement active-active concurrent write semantics. It is intended to make open source deployments safer when multiple coordinator pods accidentally or intentionally run against the same Redis metadata store.

## Store Revision

Every Redis aggregate update carries an expected `storeRevision`.

If the expected revision does not match the current revision, the write fails and the coordinator reloads and retries or returns a conflict depending on the operation.

This prevents stale snapshots from overwriting fresh heartbeat, assignment, or resharding updates.

## ACL

The MVP security model uses Basic Auth.

Roles:

| Role | Permission |
| --- | --- |
| `ADMIN` | Create, scale, rollback, update consumer concurrency |
| `MONITOR` | Read monitoring, routing, group, and resharding APIs |
| `MEMBER` | Send heartbeat when member API authentication is enabled |

Authentication failures return `401 Unauthorized`. Authorization failures return `403 Forbidden`.

## Audit Logging

Admin mutations record:

* operation type,
* stream prefix,
* consumer group,
* requested values,
* authenticated principal,
* reason,
* coordinator ID,
* timestamp,
* result.

Audit events can be written to logs or Redis.

## Rate Limiting

Admin mutation APIs can use a fixed-window rate limit keyed by authenticated principal and group. Monitoring reads and member heartbeats are excluded.

When the limit is exceeded, the coordinator returns `429 Too Many Requests` with `Retry-After`.

For multiple coordinator instances, global rate limiting should be enforced by an external gateway or load balancer if strict global limits are required.

## Monitoring APIs

Monitoring APIs are read-only. State changes must happen through Admin APIs or the coordinator event loop.

Monitoring responses should include:

* group summary,
* stream versions,
* active write version,
* readable versions,
* target assignment,
* current assignment,
* member liveness,
* member capacity,
* member-owned shard progress,
* revoke progress,
* active resharding,
* audit metadata for recent mutations.

## Metrics

Coordinator metrics are the primary observability surface. Consumer and producer modules should avoid owning long-lived metric definitions unless needed for local application debugging.

Coordinator metrics should cover:

* heartbeat requests by result,
* expired members,
* fenced members,
* group epoch and assignment epoch,
* target/current assignment size,
* pending revocations,
* duplicate owner violations,
* resharding state and duration,
* consumer shard progress and lag where available,
* producer routing requests by group,
* stale producer routing refreshes,
* store revision conflicts,
* mutex acquire latency and timeout count,
* admin mutation count and failure count.

## Alerts

Recommended alerts:

* member expiration spike,
* revoke pending age exceeds threshold,
* group epoch changed but assignment epoch does not progress,
* duplicate owner invariant violation,
* active resharding remains in progress too long,
* Redis state mutex acquire failures,
* Redis state store revision conflict spike,
* monitoring projection refresh failures,
* producer stale routing refresh spike.

## Health

Health checks should report Redis as a required dependency only when Redis-backed state, Redis audit, or Redis Stream provisioning is enabled.

If memory store is used for local development, Redis can be absent unless stream provisioning or Redis audit is enabled.
