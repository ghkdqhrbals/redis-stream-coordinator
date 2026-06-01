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

## Coordination Version

Heartbeat requests include `protocolVersion`, but the value represents the coordinator-module coordination version. It is not a heartbeat-only version.

The supported coordination version range is provided by the coordinator and starter modules, not by user YAML. Operators must not widen the accepted range manually because that can allow a version that the running code cannot actually parse or enforce.

Each coordination version entry must declare release lifecycle metadata with semantic release fields:

| Field | Meaning |
| --- | --- |
| `introducedIn.major/minor/patch` | First artifact release that introduced the version. |
| `deprecatedIn.major/minor/patch` | First release that deprecates the version, or `null` while active. |
| `minimumSupportedUntil.major/minor/patch` | Earliest release before which the version must not be removed. |
| `removedIn.major/minor/patch` | Release that removes the version, or `null` until removal is scheduled. |

Current coordinator and starter modules declare coordination version `1` as introduced in `0.1.0` and supported at least until `1.0.0`.

Policy:

* A coordinator must reject unsupported coordination versions with a clear error.
* Minor protocol additions must be optional.
* Required field changes require a major version.
* Rolling upgrades should support N/N-1 client and server coexistence through module-defined compatibility constants and release notes.

## Metadata Schema Version

The persisted DB aggregate JSON includes `schemaVersion`. Redis-backed metadata mode uses the same aggregate schema for development and tests.

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

1. Upgrade coordinator server to a version that supports both old and new coordination versions.
2. Upgrade consumer applications.
3. Upgrade producer applications.
4. Remove old protocol support only in a later major release.

Rollback should be possible while metadata schema remains compatible. If schema migration is irreversible, release notes must state that clearly.

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

## Shared Protocol Artifact

Coordination version metadata and default timing values are owned by `redisstream-core`.
The coordinator server and support modules must depend on this module instead of defining
their own heartbeat interval, member lease TTL, rebalance timeout, or supported coordination
version tables.

For coordination version `1`, the default timing contract is:

* heartbeat interval: `3s`
* member lease TTL: `15s`
* rebalance timeout: `60s`

The coordinator still sends `heartbeatIntervalMs` and `rebalanceTimeoutMs` in heartbeat responses,
and consumers should follow the server response after joining. The shared defaults exist so both artifacts behave
consistently before the first successful heartbeat and so future protocol versions can evolve
timing defaults in one place.
