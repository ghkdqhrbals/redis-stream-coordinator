# Python Client Library

The Python client lives under `clients/python` and is published as the `redisstream-coordinator` package. Its import namespace is `redisstream`.

The first release is sync-first. An asyncio namespace can be added later without changing the coordinator protocol or the Redis Stream command semantics.

## Goals

* Provide Python producer and consumer APIs that are wire-compatible with the coordinator server and the JVM starter.
* Keep official configuration small: coordinator base URL, Redis URL or Redis client, and auth token/login provider.
* Make listener concurrency behave like the JVM starter: `concurrency=N` creates N logical coordinator members.
* Keep message processing at-least-once and application-owned.
* Do not add coordinator endpoints only for Python. The library uses the existing `/coord/v1` APIs.

## Public API

```python
from redisstream import RedisStreamCoordinator

app = RedisStreamCoordinator(
    coordinator_base_url="https://coordinator.example.com",
    redis_url="redis://localhost:6379",
)

@app.stream_listener(
    stream_prefix="create-order",
    group_id="demo-workers",
    concurrency=4,
    poll_batch_size=10,
)
def handle(message):
    # Run business processing first, then explicitly commit the Redis Stream record.
    message.ack()

publisher = app.publisher("create-order", "demo-workers")
publisher.publish("order-123", {"eventId": "evt-1", "payload": "..."})

app.start()
```

The package exports:

* `RedisStreamCoordinator`
* `stream_listener`
* `CoordinatorClient`
* `ManagedConsumer`
* `RedisStreamMessage`
* `RedisStreamPublisher`
* `ProducerRoutingCache`

## Consumer Contract

`concurrency=N` creates N logical members in one Python process. The base member id is derived from `POD_IP`, hostname, then UUID fallback. For `N > 1`, the runtime appends `-m0`, `-m1`, and so on. Each logical member has its own heartbeat state, Redis consumer name, assignment state, and polling loop.

The heartbeat state machine follows the same statuses as the JVM starter:

| Status | Python client behavior |
| --- | --- |
| `OK` | Replace local assigned/pending shard view and allow reads from assigned shards. |
| `RETRY` | Keep local state and retry later. |
| `SYNC_METADATA` | Correct local metadata version and do not start newly assigned shards yet. |
| `REVOKE_PENDING` | Continue drain/revoke behavior and do not start newly assigned shards yet. |
| `UNKNOWN_MEMBER_ID` | Stop local ownership and rejoin with `memberEpoch=0`. |
| `FENCED_MEMBER_EPOCH` | Stop local ownership and rejoin with `memberEpoch=0`. |

The built-in polling adapter uses Redis `XREADGROUP`. When one logical member owns multiple shards, polling rotates shard order so later shard indexes do not starve.

The Python client does not auto-commit. Handler code explicitly calls:

* `message.ack()` for `XACK`
* `message.ack_del()` for `XACKDEL`
* `message.nack()` for `XNACK`

`XACKDEL` and `XNACK` are allowed only when Redis command support is detected. Unsupported commands raise a clear client exception.

`RedisStreamCoordinator.start()` starts registered listeners in background threads. `stop()` asks every logical member to stop and joins the listener threads. Invalid listener settings such as empty stream prefix, empty group id, zero concurrency, or non-positive poll batch size fail during configuration construction.

## Producer Contract

`ProducerRoutingCache` loads coordinator routing metadata and caches it for a bounded TTL. Routing uses the same Murmur3 32-bit hash and modulo-bias removal strategy as the JVM starter, so the same partition key and shard count map to the same shard in JVM and Python clients.

Routing metadata is validated before it is used. If the coordinator returns a different stream prefix or consumer group, a negative shard count, or a positive shard count with no shard list, the producer fails fast instead of publishing to an ambiguous target.

`RedisStreamPublisher.publish(...)` uses:

* partition-key based route selection
* `XADD NOMKSTREAM`
* configurable `MAXLEN` and approximate trimming
* fail-closed behavior when routing has `targetShardCount=0` or no shard list
* cache invalidation and refresh retry when a stale route write fails

Producers do not heartbeat. Routing refresh interval and publish retry bound propagation delay after shard scale-in/out.

## Processing Guarantee

The Python library follows the project baseline: at-least-once processing. It does not provide exactly-once side effects, global event-id deduplication, DLQ policy, or handler retry policy. Applications own idempotency, deduplication, retry, DLQ, and compensation.
