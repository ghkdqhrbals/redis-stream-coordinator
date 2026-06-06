# Testing Guide

The project keeps tests split by blast radius so contributors can run the right level locally.

## Fast Unit And Slice Tests

Run all JVM tests:

```bash
./gradlew test --no-daemon
```

Run coordinator-only tests:

```bash
./gradlew :coordinator-server:test --no-daemon
```

Run starter-only tests:

```bash
./gradlew :redisstream-spring-boot-starter:test --no-daemon
```

## Full Local Verification

```bash
./gradlew test build --no-daemon
python3 .github/scripts/test_docker_distribution.py
```

## Redis Integration Tests

Redis integration tests are disabled by default. They require an external Redis Cluster:

```bash
export AWS_REDIS_CLUSTER_NODES=3.39.42.28:6379
export AWS_REDIS_PASSWORD='your-redis-password'
```

Then run:

```bash
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test \
  --tests '*RedisCoordinatorStateStoreIntegrationTest' \
  --tests '*RedisStreamProvisioningIntegrationTest'
```

## Docker Pod Smoke

Use `compose.pods.yaml` to run the pod topology against an external Redis Cluster:

```bash
export AWS_REDIS_CLUSTER_NODES=3.39.42.28:6379
export AWS_REDIS_PASSWORD='your-redis-password'
docker compose -f compose.pods.yaml -p rsc-pods up -d --build
```

The stack starts:

* 1 coordinator on `8080`
* 2 consumer pods on `18081` and `18082`
* 1 auto-publishing pod on `18090`

Check the coordinator assignment and sample pod events:

```bash
curl -u admin:password \
  http://localhost:8080/coord/v1/monitoring/streams/create-order/groups/demo-workers/assignments

curl http://localhost:18090/sample/status
curl http://localhost:18081/sample/events
curl http://localhost:18082/sample/events
```

Open Swagger UI to exercise the same APIs from the browser:

* Coordinator: `http://localhost:8080/swagger-ui.html`
* Consumer pod 1: `http://localhost:18081/swagger-ui.html`
* Consumer pod 2: `http://localhost:18082/swagger-ui.html`
* Publisher pod: `http://localhost:18090/swagger-ui.html`

For the coordinator, click Authorize and use `admin` / `password` before calling protected `/coord/v1/**` endpoints.

Stop the stack:

```bash
docker compose -f compose.pods.yaml -p rsc-pods down
```

## Docker Smoke Test

Build the image:

```bash
docker build -t redis-stream-coordinator/coordinator-server:local .
```

Run a memory-store smoke container:

```bash
docker run --rm -p 18080:8080 \
  -e COORDINATOR_STORE_TYPE=memory \
  -e REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD=smoke-password \
  redis-stream-coordinator/coordinator-server:local
```

In another shell:

```bash
curl -u admin:smoke-password http://localhost:18080/coord/v1/monitoring/health
```

## Test Naming

Use names that describe behavior from the operator or application developer perspective:

* `member expires then remaining consumers converge on reassigned shards`
* `rate limit rejects repeated admin mutations for same caller and group`
* `redis metadata parser rejects unsupported future schema`

Prefer a small number of scenario tests around coordinator state-machine flows, backed by smaller focused tests for validation, assignment, Redis persistence, and starter callbacks.
