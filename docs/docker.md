# Docker Guide

This guide explains how to run Redis Stream Coordinator as a container.

## Local Coordinator With Redis Cluster

Start the local three-node Redis Cluster and coordinator server:

```bash
docker compose --profile coordinator up --build
```

Check coordinator health:

```bash
curl -u admin:password http://localhost:8080/coord/v1/monitoring/health
```

Expected response:

```json
{"status":"UP","coordinatorId":"local-coordinator","redis":"UP","loop":"UP"}
```

Create a group:

```bash
curl -u admin:password \
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
| `REDIS_STREAM_COORDINATOR_ADMIN_PASSWORD` | Basic Auth password for the default `admin` user. |
| `COORDINATOR_STORE_TYPE` | `redis` for Redis-backed state, `memory` for local smoke tests. |
| `COORDINATOR_STREAMS_PROVISIONING_ENABLED` | Enables Redis Stream and consumer-group provisioning. |
| `SPRING_DATA_REDIS_CLUSTER_NODES` | Comma-separated Redis Cluster seed nodes. |
| `COORDINATOR_API_RATE_LIMIT_ENABLED` | Enables admin mutation API rate limiting. |
| `COORDINATOR_AUDIT_SINK` | `log` or `redis`. |

## Redis Cluster Redirects

Redis Cluster returns node addresses in `MOVED`/`ASK` redirects. In local Docker Compose, Redis nodes advertise `127.0.0.1` so host tools such as IntelliJ and `redis-cli -c -p 7001` work. A coordinator container cannot connect to its own `127.0.0.1` for those redirects, so the compose file maps advertised addresses back to Docker service names:

```yaml
coordinator:
  redis-cluster:
    node-mappings:
      - advertised-host: 127.0.0.1
        advertised-port: 7001
        connect-host: redis-node-1
        connect-port: 7001
```

Production Redis Cluster should advertise addresses that coordinator containers can reach directly. Use `node-mappings` only when Redis advertises a different address than the coordinator should connect to.

## Publishing

The `Docker image` GitHub Actions workflow has two modes:

* Pull requests build the image and smoke test `/coord/v1/monitoring/health`.
* Manual `workflow_dispatch` publishes `ghcr.io/<owner>/<repo>/coordinator-server:<version>`.

Do not publish `latest` automatically for every commit. Use the manual workflow after release notes and compatibility notes are ready.
