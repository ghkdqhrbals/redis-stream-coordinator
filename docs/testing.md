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
