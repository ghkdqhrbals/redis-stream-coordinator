# Contributing

Thanks for contributing to Redis Stream Coordinator. This project is infrastructure code, so compatibility and tests matter as much as feature code.

## Development Setup

Requirements:

* Java 24
* Docker, for Redis Cluster and image smoke tests
* Gradle Wrapper from this repository

Run the standard verification:

```bash
./gradlew test build --no-daemon
python3 .github/scripts/test_docker_distribution.py
```

Redis integration tests:

```bash
docker compose up -d
REDIS_COORDINATOR_INTEGRATION_TESTS=true ./gradlew :coordinator-server:test \
  --tests '*RedisCoordinatorStateStoreIntegrationTest' \
  --tests '*RedisStreamProvisioningIntegrationTest'
```

## Pull Request Expectations

Include tests for behavior changes. Use the smallest effective test level:

* Unit tests for assignment, validation, routing, and state transitions.
* HTTP integration tests for API status codes, auth, ACL, audit, and rate limits.
* Redis integration tests for persistence, Lua updates, provisioning, and cluster key behavior.
* Starter tests for consumer lifecycle, producer routing, and Redis polling behavior.

Update documentation when changing:

* public Kotlin APIs
* HTTP endpoints or status codes
* heartbeat protocol fields
* Redis metadata schema
* Docker or runtime configuration
* operational behavior

## Compatibility Rules

Follow [Versioning and Compatibility Policy](docs/prd/11-versioning-compatibility.md).

Breaking changes need:

* a new major version or explicit pre-1.0 migration note
* release note entry
* compatibility tests
* Redis schema migration plan if persisted metadata changes

Do not silently overwrite Redis metadata with an unsupported future `schemaVersion`.

## Code Style

Use existing Kotlin and Spring Boot patterns in the repo. Keep coordinator logic deterministic and avoid adding data-plane processing behavior to the coordinator server.

Prefer explicit domain errors through `CoordinatorError` and `CoordinatorException`.

## Release Checklist

Before a public release:

1. Update `gradle.properties` `projectVersion`.
2. Update `CHANGELOG.md`.
3. Run `./gradlew test build --no-daemon`.
4. Run Redis integration tests.
5. Run Docker smoke test or the `Docker image` workflow.
6. Publish the coordinator image manually through the `Docker image` workflow.
7. Attach compatibility notes for artifact version, heartbeat protocol, HTTP API, and Redis metadata schema.
