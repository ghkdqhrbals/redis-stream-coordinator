# 테스트 가이드

테스트는 blast radius에 따라 나눈다. Contributor는 변경 범위에 맞는 테스트부터 실행하면 된다.

## 빠른 JVM 테스트

전체 JVM 테스트:

```bash
./gradlew test --no-daemon
```

Coordinator만:

```bash
./gradlew :coordinator-server:test --no-daemon
```

Starter만:

```bash
./gradlew :redisstream-spring-boot-starter:test --no-daemon
```

## 전체 로컬 검증

```bash
./gradlew test build --no-daemon
python3 .github/scripts/test_docker_distribution.py
```

## Redis Integration Tests

Redis integration tests는 기본 비활성화되어 있다. 외부 Redis Cluster 정보를 먼저 설정한다.

```bash
export AWS_REDIS_CLUSTER_NODES=3.39.42.28:6379
export AWS_REDIS_PASSWORD='your-redis-password'
```

그 다음:

```bash
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test \
  --tests '*RedisCoordinatorStateStoreIntegrationTest' \
  --tests '*RedisStreamProvisioningIntegrationTest'
```

## Docker Pod Smoke

외부 Redis Cluster를 바라보는 pod topology를 Docker로 실행:

```bash
export AWS_REDIS_CLUSTER_NODES=3.39.42.28:6379
export AWS_REDIS_PASSWORD='your-redis-password'
docker compose -f compose.pods.yaml -p rsc-pods up -d --build
```

Stack:

* Coordinator: `8080`
* Consumer pod 1: `18081`
* Consumer pod 2: `18082`
* Publisher pod: `18090`

확인:

```bash
curl -u admin:password \
  http://localhost:8080/coord/v1/monitoring/streams/create-order/groups/demo-workers/assignments

curl http://localhost:18090/sample/status
curl http://localhost:18081/sample/events
curl http://localhost:18082/sample/events
```

Swagger UI:

* Coordinator: `http://localhost:8080/swagger-ui.html`
* Consumer pod 1: `http://localhost:18081/swagger-ui.html`
* Consumer pod 2: `http://localhost:18082/swagger-ui.html`
* Publisher pod: `http://localhost:18090/swagger-ui.html`

중단:

```bash
docker compose -f compose.pods.yaml -p rsc-pods down
```

## Test Naming

테스트 이름은 operator 또는 application developer 관점의 behavior를 설명해야 한다.

Examples:

* `member expires then remaining consumers converge on reassigned shards`
* `rate limit rejects repeated admin mutations for same caller and group`
* `redis metadata parser rejects unsupported future schema`

큰 state-machine flow test, 작은 scenario test, validation/unit test를 분리해서 유지한다.
