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

Redis integration tests are disabled by default. Start the local Redis Cluster first:

```bash
docker compose up -d
```

Then run:

```bash
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test \
  --tests '*RedisCoordinatorStateStoreIntegrationTest' \
  --tests '*RedisStreamProvisioningIntegrationTest'
```

## Local End-To-End Smoke

Use the no-persistence E2E Redis Cluster when you want to run the coordinator and sample consumer pods locally without touching the default Compose volumes:

```bash
docker compose -f compose.e2e.yaml -p rsc-e2e up -d
```

Run the coordinator against the E2E cluster:

```bash
COORDINATOR_STORE_TYPE=redis \
COORDINATOR_STREAMS_PROVISIONING_ENABLED=true \
SPRING_DATA_REDIS_CLUSTER_NODES=localhost:7101,localhost:7102,localhost:7103 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_0_ADVERTISED_PORT=7101 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_0_CONNECT_HOST=127.0.0.1 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_0_CONNECT_PORT=7101 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_1_ADVERTISED_PORT=7102 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_1_CONNECT_HOST=127.0.0.1 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_1_CONNECT_PORT=7102 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_2_ADVERTISED_PORT=7103 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_2_CONNECT_HOST=127.0.0.1 \
COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_2_CONNECT_PORT=7103 \
./gradlew :coordinator-server:bootRun
```

Create a group, then run two sample consumer pods:

```bash
SERVER_PORT=18081 CONSUMER_MEMBER_ID=consumer-pod-1 STREAM_PREFIX=demo.orders \
REDIS_CLUSTER_NODE_1=localhost:7101 REDIS_CLUSTER_NODE_2=localhost:7102 REDIS_CLUSTER_NODE_3=localhost:7103 \
  ./gradlew :samples:consumer-pod:bootRun

SERVER_PORT=18082 CONSUMER_MEMBER_ID=consumer-pod-2 STREAM_PREFIX=demo.orders \
REDIS_CLUSTER_NODE_1=localhost:7101 REDIS_CLUSTER_NODE_2=localhost:7102 REDIS_CLUSTER_NODE_3=localhost:7103 \
  ./gradlew :samples:consumer-pod:bootRun
```

The sample exposes:

* `GET /sample/status`
* `GET /sample/events`
* `POST /sample/publish`

## Docker Pod Smoke

Use `compose.pods.yaml` to run the full local pod topology in Docker:

```bash
docker compose -f compose.pods.yaml -p rsc-pods up -d --build
```

The stack starts:

* 3 Redis Cluster nodes on host ports `7201`, `7202`, and `7203`
* 1 coordinator on `8080`
* 2 consumer pods on `18081` and `18082`
* 1 auto-publishing pod on `18090`

Check the coordinator assignment and sample pod events:

```bash
curl -u admin:password \
  http://localhost:8080/coord/v1/monitoring/streams/demo.orders/groups/demo-workers/assignments

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
