# Versioning and Compatibility Policy

## Goals

This project is intended to be used as open source infrastructure. Versioning is part of the product contract, not an implementation detail.

The project must support safe rolling upgrades where old and new coordinator/consumer artifacts coexist temporarily.

## Version Axes

The project has four separately tracked compatibility axes:

| Axis | Scope | Compatibility rule |
| --- | --- | --- |
| Artifact version | Gradle/Maven modules such as `coordinator-server` and `redisstream-spring-boot-starter` | Semantic Versioning. |
| Heartbeat protocol version | Member heartbeat request/response schema | Coordinator accepts a configured version range. |
| HTTP API version | REST path prefix such as `/coord/v1` | Breaking API changes require a new path prefix. |
| Redis metadata schema version | Persisted coordinator aggregate and projection format | Breaking schema changes require migration code and a documented upgrade path. |

## Semantic Versioning

Artifact versions follow `MAJOR.MINOR.PATCH`.

Rules:

* `PATCH`: bug fixes only. No public API, protocol, or schema breaks.
* `MINOR`: backward-compatible features. A minor release must support the previous minor line for rolling upgrades.
* `MAJOR`: allowed to remove deprecated API, drop old protocol versions, or require schema migration.

Before `1.0.0`, the public API may still evolve, but every compatibility-affecting change must still be documented in this PRD and release notes.

## Compatibility Support Window

Default support target:

* Current minor version: `N`
* Previous minor version: `N-1`

For example, `1.4.x` coordinator should accept `1.3.x` RedisStream starter protocol when the heartbeat protocol remains within the configured supported range.

Security fixes may be backported to the latest patch of the current minor line. Broader backports are best-effort until the project publishes a formal support matrix.

## Heartbeat Protocol Version

Heartbeat schema compatibility is controlled by `protocolVersion`.

Coordinator config:

```yaml
coordinator:
  protocol:
    min-heartbeat-version: 1
    max-heartbeat-version: 1
```

Rules:

* Coordinator rejects unsupported versions with `UNSUPPORTED_PROTOCOL`.
* Optional fields may be added without increasing the protocol version when old clients can omit them and old servers can ignore them.
* Required request fields, removed response fields, changed enum semantics, or changed fencing behavior require a new heartbeat protocol version.
* During a rolling upgrade, deploy the coordinator with a supported range that covers both old and new clients.

## HTTP API Version

The current REST API prefix is:

```text
/coord/v1
```

Rules:

* Backward-compatible fields may be added to `v1` responses.
* Removing fields, changing meaning, changing status codes, or changing endpoint semantics requires `/coord/v2`.
* Old endpoint versions should remain available for at least one minor release after a new endpoint version is introduced.

## Redis Metadata Schema Version

Redis-backed state includes explicit schema version metadata.

Rules:

* Additive projection keys are minor-version compatible.
* Removing or renaming keys requires migration code.
* A coordinator must not silently overwrite a newer unknown schema.
* Schema migration should be idempotent and observable.

Current MVP status:

* Redis store uses aggregate/projection keys and `storeRevision` CAS.
* Group aggregate metadata includes `schemaVersion=1`.
* Coordinator rejects unsupported future schema versions instead of overwriting them.
* Legacy group aggregate metadata without `schemaVersion` is treated as version `1`.

## Deprecation Policy

Deprecations must include:

* first deprecated version
* replacement API or behavior
* earliest removal version
* migration note

Minimum removal window after `1.0.0`:

* public Kotlin API: one minor release
* HTTP endpoint version: one minor release
* heartbeat protocol version: one minor release after coordinator supports the replacement
* Redis schema: one major release unless a security or correctness issue requires faster removal

## Release Checklist

Every release should update:

* `gradle.properties` `projectVersion`
* compatibility matrix in release notes
* supported heartbeat protocol range
* migration notes for Redis schema changes
* deprecation list
* test result summary for coordinator and RedisStream starter modules
* Docker image smoke result and published GHCR image tag, when releasing the coordinator image

## Required Tests

Versioning changes must include tests for:

* coordinator accepts the oldest supported heartbeat protocol version
* coordinator accepts the newest supported heartbeat protocol version
* coordinator rejects below-minimum and above-maximum heartbeat versions
* RedisStream starter fails fast when configured with a locally unsupported protocol version
* rolling-upgrade behavior when old and new members heartbeat against the same group
