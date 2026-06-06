# Docker Guide

This guide explains how to run Redis Stream Coordinator as a container.

## Local Coordinator With External Redis Cluster

Start the coordinator and sample pod topology against an external Redis Cluster:

```bash
export AWS_REDIS_CLUSTER_NODES=3.39.42.28:6379
export AWS_REDIS_PASSWORD='your-redis-password'
docker compose -f compose.pods.yaml -p rsc-pods up -d --build
```

Check coordinator health:

```bash
RSC_TOKEN="$(
  curl -sS -H 'Content-Type: application/json' \
    -X POST http://localhost:8080/coord/v1/auth/login \
    -d '{"username":"admin","password":"password"}' |
  jq -r '.accessToken'
)"

curl -H "Authorization: Bearer ${RSC_TOKEN}" \
  http://localhost:8080/coord/v1/monitoring/health
```

The repository intentionally does not keep a local Redis Cluster compose file. Local Docker runs should use the same Redis Cluster style as production by setting `AWS_REDIS_CLUSTER_NODES` and `AWS_REDIS_PASSWORD`.

Create a group:

```bash
curl -H "Authorization: Bearer ${RSC_TOKEN}" \
  -H 'Content-Type: application/json' \
  -X POST http://localhost:8080/coord/v1/streams/orders/groups/orders-consumer \
  -d '{"initialShardCount":4,"requestedBy":"local","reason":"local bootstrap"}'
```

## Build Image

```bash
docker build -t redis-stream-coordinator/coordinator-server:local .
```

The image uses Java 24, runs as a non-root user, and starts `coordinator-server`.

## Runtime Configuration

The coordinator image uses the same Spring Boot configuration keys as the jar. Important environment variables:

| Environment variable | Purpose |
| --- | --- |
| `REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD` | Password used by `/coord/v1/auth/login` for the default `admin` user. Basic Auth remains accepted for compatibility. |
| `REDIS_STREAM_COORDINATOR_TOKEN_SECRET` | HMAC signing secret for seven-day Bearer tokens. Set this explicitly in production. |
| `REDIS_STREAM_COORDINATOR_TOKEN_TTL` | Bearer token lifetime. Default is `7d`. |
| `COORDINATOR_STORE_TYPE` | `redis` for Redis-backed state, `memory` for local smoke tests. |
| `COORDINATOR_STREAMS_PROVISIONING_ENABLED` | Enables Redis Stream and consumer-group provisioning. |
| `SPRING_DATA_REDIS_CLUSTER_NODES` | Comma-separated Redis Cluster seed nodes. |
| `COORDINATOR_API_RATE_LIMIT_ENABLED` | Enables admin mutation API rate limiting. |
| `COORDINATOR_AUDIT_SINK` | `log` or `redis`. |

## Publishing

The `Docker image` GitHub Actions workflow has two modes:

* Pull requests build the image and smoke test `/coord/v1/monitoring/health`.
* Manual `workflow_dispatch` publishes `ghcr.io/<owner>/<repo>/coordinator-server:<version>`.

Do not publish `latest` automatically for every commit. Use the manual workflow after release notes and compatibility notes are ready.
