# Versioning and Compatibility Policy

## Why Versioning Matters

This project is intended for open source use. Users can run different versions of the coordinator server, consumer starter, producer starter, Docker image, and Redis server. Compatibility must be explicit so upgrades can be planned safely.

## Artifact Versioning

Artifacts follow Semantic Versioning:

* `MAJOR`: incompatible API, protocol, or metadata changes,
* `MINOR`: backward-compatible features,
* `PATCH`: bug fixes and documentation updates.

Artifacts:

* `coordinator-server`,
* `redisstream-spring-boot-starter`,
* sample Docker images,
* published coordinator Docker image.

## HTTP API Versioning

The base path includes a version:

```text
/coord/v1
```

Breaking HTTP API changes require a new path prefix such as `/coord/v2`.

Backward-compatible additions are allowed in the same API version:

* optional request fields,
* additive response fields,
* new enum values only when old clients ignore unknown values safely,
* new monitoring endpoints.

## Heartbeat Protocol Version

Heartbeat requests include `protocolVersion`.

The coordinator configuration defines an accepted range, for example:

```yaml
coordinator:
  protocol:
    min-supported-version: "1.0"
    max-supported-version: "1.1"
```

Policy:

* A coordinator must reject unsupported protocol versions with a clear error.
* Minor protocol additions must be optional.
* Required field changes require a major version.
* Rolling upgrades should support N/N-1 client and server coexistence.

## Redis Metadata Schema Version

Redis aggregate state includes `schemaVersion`.

Policy:

* The coordinator validates schema version before reading or writing.
* Incompatible schema changes require migration notes.
* Compatible additive fields require default handling for older state.
* Tests must cover reading old schema fixtures where support is promised.

## Docker Image Tags

Recommended tags:

* immutable version tag, for example `v0.1.0`,
* commit SHA tag,
* optional moving tag such as `unstable` for pre-release builds.

`latest` should be avoided until the first public release policy is stable.

## Redis Version Compatibility

The modules should check Redis server version before using version-specific commands.

Documentation must state:

* minimum supported Redis version,
* tested Redis versions,
* command compatibility notes,
* fallback behavior for unsupported commands.

## Upgrade Policy

Recommended rolling upgrade order:

1. Upgrade coordinator server to a version that supports both old and new heartbeat protocols.
2. Upgrade consumer applications.
3. Upgrade producer applications.
4. Remove old protocol support only in a later major release.

Rollback should be possible while Redis metadata schema remains compatible. If schema migration is irreversible, release notes must state that clearly.

## Compatibility Test Matrix

Required tests:

* old client heartbeat against new coordinator,
* new client heartbeat against old supported coordinator where applicable,
* old producer routing response parsing,
* schema fixture read tests,
* Docker smoke test for published image,
* Redis command compatibility tests for configured ACK and publish modes.

## Deprecation Policy

Deprecations must include:

* replacement behavior,
* first version where deprecation appears,
* earliest removal version,
* migration notes,
* warning logs or metrics where practical.
