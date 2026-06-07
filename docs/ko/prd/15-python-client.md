# Python Client Library

Python client는 `clients/python` 아래에 위치하며 패키지 이름은 `redisstream-coordinator`, import namespace는 `redisstream`이다.

1차 버전은 sync-first로 제공한다. asyncio 지원은 이후 `redisstream.asyncio` namespace로 확장할 수 있으며, coordinator protocol과 Redis Stream command semantics는 동일하게 유지한다.

## 목표

* Python producer/consumer API를 coordinator server 및 JVM starter와 wire-compatible하게 제공한다.
* 공식 설정은 coordinator base URL, Redis URL 또는 Redis client, 인증 token/login provider로 최소화한다.
* Listener concurrency는 JVM starter와 동일하게 동작한다. `concurrency=N`은 N개의 logical coordinator member를 만든다.
* Message processing 보장은 at-least-once이며, 비즈니스 idempotency와 retry/DLQ 정책은 애플리케이션 책임이다.
* Python만을 위한 coordinator endpoint는 추가하지 않는다. 기존 `/coord/v1` API를 사용한다.

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

패키지는 다음 API를 export한다.

* `RedisStreamCoordinator`
* `stream_listener`
* `CoordinatorClient`
* `ManagedConsumer`
* `RedisStreamMessage`
* `RedisStreamPublisher`
* `ProducerRoutingCache`

## Consumer Contract

`concurrency=N`은 하나의 Python process 안에 N개의 logical member를 만든다. base member id는 `POD_IP`, hostname, UUID fallback 순서로 생성한다. `N > 1`이면 `-m0`, `-m1` suffix를 붙인다. 각 logical member는 별도 heartbeat state, Redis consumer name, assignment state, polling loop를 가진다.

Heartbeat state machine은 JVM starter와 동일한 status를 따른다.

| Status | Python client 동작 |
| --- | --- |
| `OK` | local assigned/pending shard view를 교체하고 assigned shard read를 허용한다. |
| `RETRY` | local state를 유지하고 나중에 재시도한다. |
| `SYNC_METADATA` | local metadata version을 보정하고 신규 shard read는 시작하지 않는다. |
| `REVOKE_PENDING` | drain/revoke를 계속하고 신규 shard read는 시작하지 않는다. |
| `UNKNOWN_MEMBER_ID` | local ownership을 중지하고 `memberEpoch=0`으로 rejoin한다. |
| `FENCED_MEMBER_EPOCH` | local ownership을 중지하고 `memberEpoch=0`으로 rejoin한다. |

Built-in polling adapter는 Redis `XREADGROUP`을 사용한다. 하나의 logical member가 여러 shard를 소유하면 shard order를 round-robin으로 순회해서 뒤쪽 shard가 굶지 않도록 한다.

Python client는 자동 commit을 하지 않는다. Handler code가 직접 호출한다.

* `message.ack()`는 `XACK`
* `message.ack_del()`은 `XACKDEL`
* `message.nack()`은 `XNACK`

`XACKDEL`과 `XNACK`은 Redis command support가 확인된 경우에만 허용한다. 미지원 명령은 명확한 client exception으로 실패한다.

`RedisStreamCoordinator.start()`는 등록된 listener를 background thread로 실행한다. `stop()`은 모든 logical member에 정지를 요청하고 listener thread를 join한다. 빈 stream prefix, 빈 group id, 0 이하 concurrency, 0 이하 poll batch size 같은 잘못된 listener 설정은 configuration construction 시점에 실패한다.

## Producer Contract

`ProducerRoutingCache`는 coordinator routing metadata를 가져와 제한된 TTL 동안 cache한다. Routing은 JVM starter와 동일한 Murmur3 32-bit hash 및 modulo-bias 제거 방식을 사용한다. 따라서 같은 partition key와 같은 shard count에서는 JVM/Python client가 같은 shard를 선택해야 한다.

Routing metadata는 사용 전에 검증한다. Coordinator가 다른 stream prefix나 consumer group을 반환하거나, 음수 shard count를 반환하거나, 양수 shard count인데 shard list가 비어 있으면 producer는 모호한 대상에 publish하지 않고 fail-fast한다.

`RedisStreamPublisher.publish(...)`는 다음 규칙을 따른다.

* partition key 기반 route 선택
* `XADD NOMKSTREAM`
* configurable `MAXLEN` 및 approximate trimming
* routing의 `targetShardCount=0` 또는 shard list empty면 fail closed
* stale route write 실패 시 cache invalidate 후 refresh retry

Producer는 heartbeat를 보내지 않는다. Routing refresh interval과 publish retry로 shard scale-in/out 이후 전파 지연을 제한한다.

## Processing Guarantee

Python library도 프로젝트 기본 보장인 at-least-once를 따른다. Exactly-once side effect, global event-id deduplication, DLQ policy, handler retry policy를 강제하지 않는다. Idempotency, deduplication, retry, DLQ, compensation은 애플리케이션 책임이다.
