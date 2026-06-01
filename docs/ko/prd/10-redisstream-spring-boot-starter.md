# RedisStream Spring Boot Starter and Integration Contract

## 목표

`com.redisstream:redisstream-spring-boot-starter`는 애플리케이션이 Redis Stream Coordinator에 연결하기 위해 필요한 런타임 구성요소를 제공한다.

Coordinator 서버는 control plane이다. 애플리케이션은 starter를 통해 coordinator group에 join하고, heartbeat를 보내고, shard assignment를 받고, local shard worker를 시작하거나 중지하며, revoke/drain progress를 보고한다.

Starter는 특정 Redis Stream 처리 프레임워크를 강제하지 않는다. 애플리케이션은 shard lifecycle callback을 직접 구현할 수 있고, 원하면 built-in Redis Stream polling adapter를 사용할 수 있다. 비즈니스 handler 실행, retry, DLQ, idempotency, transaction boundary는 애플리케이션이 소유한다.

## 공개 통합 API

애플리케이션 모듈은 Spring Boot starter package만 의존하면 된다. Coordinator server 내부의 mutex guard, aspect, critical-section annotation은 구현 디테일이며 공개 API가 아니다.

애플리케이션이 직접 사용하는 주요 API는 다음 정도로 제한한다.

| API | 역할 |
| --- | --- |
| `@StreamConfiguration` | coordinator-managed listener method가 공유할 polling 설정과 poller thread pool size 기본값을 선언한다. |
| `@StreamListener` | business handler method를 listener endpoint로 표시하고 stream prefix, group ID, startup, concurrency를 endpoint 단위로 설정한다. |
| `CoordinatorConsumerProperties.consumer(...)` | 하나의 `{streamPrefix, consumerGroupName}`에 대한 managed consumer를 등록한다. |
| `ProducerRoutingProperties.producer(...)` | 하나의 `{streamPrefix, consumerGroupName}`에 대한 producer routing cache를 등록한다. |
| `RedisStreamPublisher.publish(...)` | partition key를 active Redis Stream shard로 routing하고 record를 `XADD`한다. |
| `CoordinatorShardLifecycle` | 애플리케이션이 직접 worker를 운영할 때 shard assign/revoke callback을 받는다. |
| `RedisStreamMessageHandler` | built-in polling adapter가 business handler를 호출한다. ACK, ACKDEL, NACK은 애플리케이션 코드가 명시적으로 수행한다. |

최소 annotation 기반 consumer 등록 예시는 다음과 같다.

```kotlin
@StreamConfiguration
class OrdersConsumer {
    @StreamListener(
        id = "orders-consumer-a",
        streamPrefix = "orders",
        groupId = "orders-consumer",
        concurrency = "4",
    )
    fun consume(message: ConsumedRedisStreamMessage) {
        // Business processing.
        message.ack()
    }
}
```

Annotation listener에서 `concurrency = "4"`는 같은 애플리케이션 프로세스 안에 4개의 논리 coordinator member를 만든다. 각 논리 member는 개별 `memberId`, heartbeat loop, Redis consumer name, assignment state, shard ownership을 가진다. 이 방식이 Kafka와 유사한 listener concurrency 모델이다.

Annotation listener는 항상 이 logical-member split 모델을 사용한다. Starter는 split toggle을 공개 설정으로 제공하지 않으며, `concurrency = "4"`가 하나의 coordinator member 내부 local poller 네 개를 의미하는 모드는 지원하지 않는다.

Shard ownership과 member concurrency는 별개이다. Coordinator는 각 shard를 정확히 하나의 live member에만 배치하지만, 하나의 member가 여러 shard를 소유할 수 있다. Listener concurrency는 assignment에 참여하는 논리 member 수를 늘리는 기능이며, shard 하나는 live owner 하나만 가진다는 규칙은 바꾸지 않는다.

Annotation 기반 consumer에서는 starter가 runtime `memberId`를 자동 생성한다. `id`는 listener endpoint 식별자이며 coordinator member ID가 아니다.

Heartbeat interval과 rebalance timeout은 shared coordination version timing 기본값을 사용한다. `rebalanceTimeout`은 revoke된 shard를 drain할 수 있도록 coordinator가 해당 member를 기다리는 최대 시간이다. 이 시간 안에 revoke 완료 보고가 오지 않으면 coordinator는 member를 fence하고 shard를 재할당할 수 있다.

Bean 기반 consumer/producer 등록 예시는 다음과 같다.

```kotlin
@Bean
fun ordersConsumer(): CoordinatorConsumerProperties =
    CoordinatorConsumerProperties.consumer("orders", "orders-consumer") {
        runtimeMaxConcurrency = 4
    }

@Bean
fun ordersProducer(): ProducerRoutingProperties =
    ProducerRoutingProperties.producer("orders", "orders-consumer") {
        xadd.maxLen = 100_000
    }
```

Bean 기반 경로에서 `runtimeMaxConcurrency = 4`는 하나의 coordinator member에 대한 local worker capacity이다. 네 개의 member ID를 만들지는 않는다. Kafka와 유사한 member fan-out이 필요하면 `@StreamListener(concurrency = "N")`을 사용하거나 서로 다른 member identity를 가진 managed consumer를 명시적으로 여러 개 등록해야 한다.

최소 publish 예시는 다음과 같다.

```kotlin
redisStreamPublisher.publish(
    partitionKey = "order-123",
    fields = mapOf("eventId" to "evt-1", "payload" to payload),
)
```

## Public YAML Contract

공식적으로 제공하는 YAML 설정은 coordinator endpoint 하나만이다.

```yaml
redis-stream-coordinator:
  coordinator-base-url: http://localhost:8080
```

Consumer/producer의 stream prefix, consumer group name, polling size, timeout, ACK 정책, producer routing refresh, XADD MAXLEN 같은 runtime 설정은 코드에서 bean으로 정의한다. 이렇게 해야 한 애플리케이션 안에서 여러 consumer/producer를 명시적으로 만들 수 있고, 애플리케이션 고유 설정 시스템이나 버전 정책과 충돌하지 않는다.

## Consumer 설정

`consumerGroupName`은 Redis Stream consumer group의 논리 이름이다. Starter는 `member-name`을 public YAML 설정으로 제공하지 않는다. Heartbeat protocol compatibility에 필요한 member display field는 내부적으로 `consumerGroupName`에서 파생한다.

Managed consumer bean이 생성될 때 starter는 설정된 `streamPrefix`와 `consumerGroupName`에 대한 coordinator routing metadata를 먼저 조회한다. Group이 없거나, `shardCount`가 0이거나, active shard metadata가 불완전하면 애플리케이션 시작 시점에 실패한다.

```kotlin
@Configuration(proxyBeanMethods = false)
class OrdersConsumerConfiguration {
    @Bean
    fun ordersConsumerProperties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties.consumer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
        ) {
            runtimeMaxConcurrency = 4
            heartbeatInterval = Duration.ofSeconds(3)
            rebalanceTimeout = Duration.ofSeconds(60)
            gracefulLeaveOnStop = true
        }
}
```

Starter가 담당하는 영역:

* heartbeat scheduling
* coordinator HTTP call
* `memberEpoch`와 `metadataVersion` tracking
* `ownedShards`와 `revokingShards` reporting
* assignment diffing
* assign, pending, revoke, fenced callback 호출
* runtime capacity reporting
* built-in polling 사용 시 shard progress reporting

Heartbeat status 처리:

| Status | Starter 동작 |
| --- | --- |
| `OK` | Assignment를 적용하고 신규 assigned shard worker를 시작한다. |
| `SYNC_METADATA` | Local metadata version을 갱신하고, 현재 owned shard 중 coordinator가 계속 assigned로 표시한 shard만 유지한다. 나머지는 revoke/drain하며 신규 shard는 시작하지 않는다. |
| `REVOKE_PENDING` | Revoke/drain을 계속하고, 이후 `OK`가 올 때까지 신규 shard를 pending 상태로 둔다. |
| `RETRY` | Local ownership을 유지하고 full state로 재시도한다. |
| `UNKNOWN_MEMBER_ID` / `FENCED_MEMBER_EPOCH` | Local ownership을 중단하고 rejoin한다. |

Starter가 직접 소유하지 않는 영역:

* payload deserialization
* 비즈니스 handler transaction
* 외부 DB/API side effect
* DLQ와 retry policy
* domain-level idempotency

## Shard Lifecycle API

애플리케이션은 `CoordinatorShardLifecycle`을 구현한다.

```kotlin
@Component
class OrdersShardLifecycle : CoordinatorShardLifecycle {
    override fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
        // assigned shard worker를 시작한다.
    }

    override fun onRevoked(
        shards: Set<CoordinatorShard>,
        context: CoordinatorConsumerContext,
    ): Set<CoordinatorShard> {
        // 새 read를 멈추고 in-flight work를 drain한 뒤 완료된 shard를 반환한다.
        return shards
    }
}
```

Lifecycle callback은 idempotent해야 한다. reconnect, retry, heartbeat 중복으로 같은 assignment가 두 번 이상 전달될 수 있다.

## Built-in Redis Stream Polling

Starter가 Redis Stream read를 수행하게 하려면 `RedisStreamMessageHandler` bean을 제공하고, polling 옵션을 코드에서 설정한다.

```kotlin
@Configuration(proxyBeanMethods = false)
class OrdersPollingConfiguration {
    @Bean
    fun ordersConsumerProperties(): CoordinatorConsumerProperties =
        CoordinatorConsumerProperties.consumer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
        ) {
            redis.pollBatchSize = 10
            redis.pollTimeout = Duration.ofSeconds(1)
        }
}
```

```kotlin
@Component
class OrdersMessageHandler : RedisStreamMessageHandler {
    override fun handle(message: ConsumedRedisStreamMessage) {
        // 비즈니스 처리가 성공한 뒤 애플리케이션 코드가 직접 ACK한다.
        message.ack()
    }
}
```

Built-in polling adapter는 자동 ACK를 수행하지 않는다. 비즈니스 side effect가 성공한 뒤 `message.ack()`를 호출하고, stream entry까지 삭제해야 하는 경우 `message.ackDel()`을 호출한다. Redis XNACK을 사용해야 하는 경우에는 `message.nack()`을 명시적으로 호출한다.

Polling semantics:

* coordinator ownership은 shard 단위이며, 하나의 shard는 최대 하나의 live member만 읽을 수 있다.
* listener concurrency는 애플리케이션 프로세스 안에 생성할 논리 member 수이다.
* bean 기반 `runtimeMaxConcurrency`는 하나의 논리 member가 가진 local worker capacity이다.
* 하나의 member가 여러 shard를 소유하면 worker가 owned shard를 round-robin으로 순회한다.
* 뒤쪽 shard index도 계속 poll/processing되어야 하며, 하나의 member가 worker 수보다 많은 shard를 소유했다는 이유로 영구적인 shard starvation이 발생하면 안 된다.
* built-in adapter는 shard-local ordering과 중복 local read 방지를 위해 shard당 하나의 active poll만 허용한다.

Failed-message 처리는 `redis.failure.mode`로 별도 설정한다. 기본값은 `LEAVE_PENDING`이며 classic Redis Stream retry behavior를 유지한다. `XNACK`은 명시적으로 설정하고 Redis 버전이 지원할 때만 사용한다.

## Consumer Progress Reporting

Built-in polling lifecycle은 heartbeat에 shard별 progress를 포함할 수 있다.

* last delivered Redis Stream ID
* last acknowledged Redis Stream ID
* pending count
* in-flight count
* shard read state

Coordinator는 이 값을 monitoring과 drain 판단에 사용한다. 이 값은 exactly-once 보장이 아니다.

## Producer 설정

Producer도 YAML에는 shared coordinator endpoint만 둔다. Routing identity와 producer 동작은 코드에서 설정한다.

```kotlin
@Configuration(proxyBeanMethods = false)
class OrdersProducerConfiguration {
    @Bean
    fun ordersProducerProperties(): ProducerRoutingProperties =
        ProducerRoutingProperties.producer(
            streamPrefix = "orders",
            consumerGroupName = "orders-consumer",
        ) {
            routingRefreshInterval = Duration.ofSeconds(30)
            publishMaxAttempts = 1
            xadd.maxLen = 10_000_000
            xadd.approximateTrimming = true
        }
}
```

`ProducerRoutingCache` bean도 생성 시 같은 초기 metadata validation을 수행하고 local routing cache를 채운다. Prefix/group shard metadata가 없는 상태는 첫 publish 시점의 에러가 아니라 startup error이다.

Producer는 heartbeat를 보내지 않는다. Shard 추가와 shard count 변경은 주기적 routing metadata refresh로 producer에 전파된다. `routingRefreshInterval`은 일반적인 전파 지연 상한이고, routing cache lease는 coordinator refresh 없이 producer가 계속 publish할 수 있는 최대 시간이다. Cache lease가 만료된 뒤에도 refresh가 실패하면 stale routing을 무기한 사용하지 않고 publish를 fail closed해야 한다.

애플리케이션은 `RedisStreamPublisher`로 publish한다.

```kotlin
redisStreamPublisher.publish(
    partitionKey = "order-123",
    fields = mapOf("eventId" to "evt-1", "payload" to "..."),
)
```

Publisher는 coordinator의 producer routing metadata를 읽고, shard count와 shard metadata를 기준으로 partition key를 stream shard에 매핑한 뒤 `XADD`한다.

Producer path는 global event id deduplication을 제공하지 않는다. Shard scale-out/in으로 routing metadata가 바뀌면 같은 event id가 서로 다른 shard에 쓰일 수 있다. 중복에 민감한 workload는 scale 작업 동안 producer를 pause하고 in-flight XADD와 retry window를 drain한 뒤 routing metadata를 refresh해야 한다.

## Redis Command Template

Built-in producer와 consumer adapter가 Redis에 보내는 명령은 `RedisStreamCommandsTemplate`을 통해 중앙화한다.

Template가 담당하는 명령:

* `XREADGROUP`
* `XACK`
* `XACKDEL`
* `XNACK`
* `XADD`
* Redis server version lookup

이렇게 하면 Redis command shape, version compatibility check, serialization behavior를 한 곳에서 테스트할 수 있다.

## Redis Version Compatibility

Starter는 version-specific command를 사용하기 전에 Redis server version을 확인한다.

* `ack.mode=AUTO`는 Redis가 지원하면 `XACKDEL`, 아니면 `XACK`으로 resolve한다.
* `ack.mode=XACKDEL`은 Redis가 지원하지 않으면 ACK 명령을 보내기 전에 실패한다.
* `failure.mode=XNACK`은 Redis 8.8+ 지원이 확인된 경우에만 사용한다.

## Metrics Boundary

Open-source 사용자를 위한 공식 운영 지표는 coordinator가 제공한다. Starter는 consumer/producer Micrometer meter를 자동 등록하지 않고, `redis-stream-coordinator.consumer.metrics`나 `redis-stream-coordinator.producer.metrics` 설정을 제공하지 않는다.

Consumer progress는 heartbeat로 coordinator에 전달되고, coordinator monitoring API와 coordinator metrics로 노출된다.

## Processing Guarantee

Starter의 기본 보장은 at-least-once processing이다.

중복 attempt는 다음 상황에서 발생할 수 있다.

* Redis가 message를 반환한 뒤 process가 `XACK` 전에 crash
* pending entry를 다른 consumer가 reclaim
* Redis `XADD` 결과가 불확실한 상태에서 producer retry
* shard scale-out/in 중 producer routing metadata 변경
* handler가 외부 DB/API side effect 일부만 성공한 뒤 실패

Starter는 single-processing guarantee를 주장하지 않는다. 중복 side effect를 허용할 수 없는 애플리케이션은 domain-level idempotency, deduplication, unique constraint, compensation을 직접 구현해야 한다.

## Compatibility Contract

Starter는 모듈이 정의한 `protocolVersion`을 heartbeat request에 포함한다. 이 값은 heartbeat 전용 version이 아니라 coordinator-module coordination version이다. Coordinator server도 모듈이 정의한 supported coordination version range만 허용한다. 애플리케이션이나 운영 YAML에서 coordination version을 직접 설정하지 않는다.

Starter는 coordination version release lifecycle metadata도 제공한다. 각 coordination version은 도입된 semantic release, 최소 지원 보장 release, 선택적인 deprecation/removal release를 명시한다.

Breaking change에는 major version bump, migration guide, N/N-1 compatibility test가 필요하다.
