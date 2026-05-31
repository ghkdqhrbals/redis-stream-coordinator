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
| `CoordinatorConsumerProperties.consumer(...)` | 하나의 `{streamPrefix, consumerGroupName}`에 대한 managed consumer를 등록한다. |
| `ProducerRoutingProperties.producer(...)` | 하나의 `{streamPrefix, consumerGroupName}`에 대한 producer routing cache를 등록한다. |
| `RedisStreamPublisher.publish(...)` | partition key를 active Redis Stream shard로 routing하고 record를 `XADD`한다. |
| `CoordinatorShardLifecycle` | 애플리케이션이 직접 worker를 운영할 때 shard assign/revoke callback을 받는다. |
| `RedisStreamMessageHandler` | built-in polling adapter가 message를 처리하고 성공 후 ACK하도록 handler를 제공한다. |

최소 consumer/producer 등록 예시는 다음과 같다.

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
            memberId = System.getenv("HOSTNAME") ?: UUID.randomUUID().toString()
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
            redis.ack.mode = RedisStreamAckMode.AUTO
            redis.failure.mode = RedisStreamFailureMode.LEAVE_PENDING
        }
}
```

```kotlin
@Component
class OrdersMessageHandler : RedisStreamMessageHandler {
    override fun handle(message: ConsumedRedisStreamMessage) {
        // 이 메서드가 성공적으로 반환된 뒤 starter가 ACK를 수행한다.
    }
}
```

ACK mode:

| Mode | 동작 |
| --- | --- |
| `AUTO` | 연결된 Redis가 지원하면 `XACKDEL`, 아니면 `XACK` 사용 |
| `XACKDEL` | handler 성공 후 ACK와 delete 수행 |
| `XACK` | handler 성공 후 ACK만 수행 |

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

Producer는 heartbeat를 보내지 않는다. Shard 추가, shard count 변경, active write version 변경은 주기적 routing metadata refresh로 producer에 전파된다. `routingRefreshInterval`은 일반적인 전파 지연 상한이고, routing cache lease는 coordinator refresh 없이 producer가 계속 publish할 수 있는 최대 시간이다. Cache lease가 만료된 뒤에도 refresh가 실패하면 stale routing을 무기한 사용하지 않고 publish를 fail closed해야 한다.

애플리케이션은 `RedisStreamPublisher`로 publish한다.

```kotlin
redisStreamPublisher.publish(
    partitionKey = "order-123",
    fields = mapOf("eventId" to "evt-1", "payload" to "..."),
)
```

Publisher는 coordinator의 producer routing metadata를 읽고, active stream version과 shard metadata를 기준으로 partition key를 stream shard에 매핑한 뒤 `XADD`한다.

Producer path는 global event id deduplication을 제공하지 않는다. Shard scale-out/in으로 routing metadata가 바뀌면 같은 event id가 다른 shard 또는 stream version에 쓰일 수 있다. 중복에 민감한 workload는 scale 작업 동안 producer를 pause하고 in-flight XADD와 retry window를 drain한 뒤 routing metadata를 refresh해야 한다.

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
