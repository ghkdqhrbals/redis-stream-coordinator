# Versioning and Compatibility Policy

## Goals

This project is intended to be used as open source infrastructure. Versioning is part of the product contract, not an implementation detail.

The project must support safe rolling upgrades where old and new coordinator/consumer artifacts coexist temporarily.

## Version Axes

The project has four separately tracked compatibility axes:

| Axis | Scope | Compatibility rule |
| --- | --- | --- |
| Artifact version | Gradle/Maven modules such as `coordinator-server` and `redisstream-spring-boot-starter` | Semantic Versioning. |
| Coordination version | Coordinator server and starter/support module compatibility | Coordinator and starter modules provide the supported version range. |
| HTTP API version | REST path prefix such as `/coord/v1` | Breaking API changes require a new path prefix. |
| Metadata schema version | Persisted coordinator aggregate and projection format | Breaking schema changes require migration code and a documented upgrade path. |

## Semantic Versioning

Artifact versions follow `MAJOR.MINOR.PATCH`.

Rules:

* `PATCH`: bug fixes only. No public API, protocol, or schema breaks.
* `MINOR`: backward-compatible features. A minor release must support the previous minor line for rolling upgrades.
* `MAJOR`: allowed to remove deprecated API, drop old coordination versions, or require schema migration.

Before `1.0.0`, the public API may still evolve, but every compatibility-affecting change must still be documented in this PRD and release notes.

## Compatibility Support Window

Default support target:

* Current minor version: `N`
* Previous minor version: `N-1`

For example, `1.4.x` coordinator should accept `1.3.x` RedisStream starter coordination version when the module-defined range covers both versions.

Security fixes may be backported to the latest patch of the current minor line. Broader backports are best-effort until the project publishes a formal support matrix.

## Coordination Version

Coordinator-module compatibility is controlled by `protocolVersion` in heartbeat requests. The value is the coordination version between the coordinator server and support modules, not a heartbeat-only version.

The supported coordination version range is part of the coordinator and starter module contract. It is intentionally not exposed as YAML because an operator should not be able to advertise support for a version that the running code does not implement.

Each coordination version entry declares release lifecycle metadata with semantic release fields:

| Field | Meaning |
| --- | --- |
| `introducedIn.major/minor/patch` | First artifact release that introduced the version. |
| `deprecatedIn.major/minor/patch` | First release that deprecates the version, or `null` while active. |
| `minimumSupportedUntil.major/minor/patch` | Earliest release before which the version must not be removed. |
| `removedIn.major/minor/patch` | Release that removes the version, or `null` until removal is scheduled. |

Current coordinator and starter modules declare coordination version `1` as introduced in `0.1.0` and supported at least until `1.0.0`.

Rules:

* Coordinator rejects unsupported versions with `UNSUPPORTED_PROTOCOL`.
* Optional fields may be added without increasing the coordination version when old clients can omit them and old servers can ignore them.
* Required request fields, removed response fields, changed enum semantics, or changed fencing behavior require a new coordination version.
* During a rolling upgrade, use coordinator and starter artifact versions whose module-defined supported ranges and lifecycle metadata cover both old and new clients.

## HTTP API Version

The current REST API prefix is:

```text
/coord/v1
```

Rules:

* Backward-compatible fields may be added to `v1` responses.
* Removing fields, changing meaning, changing status codes, or changing endpoint semantics requires `/coord/v2`.
* Old endpoint versions should remain available for at least one minor release after a new endpoint version is introduced.

## Metadata Schema Version

Redis-backed aggregate JSON includes explicit schema version metadata.

Rules:

* Additive metadata aggregate fields are minor-version compatible.
* Removing or renaming fields requires migration code.
* A coordinator must not silently overwrite a newer unknown schema.
* Schema migration should be idempotent and observable.

Current MVP status:

* Coordinator store uses one Redis metadata hash per group with aggregate JSON and `storeRevision` CAS.
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
* coordination version: one minor release after coordinator supports the replacement
* Redis schema: one major release unless a security or correctness issue requires faster removal

## Release Checklist

Every release should update:

* `gradle.properties` `projectVersion`
* compatibility matrix in release notes
* supported coordination version range
* migration notes for metadata schema changes
* deprecation list
* test result summary for coordinator and RedisStream starter modules
* Docker image smoke result and published GHCR image tag, when releasing the coordinator image

## Required Tests

Versioning changes must include tests for:

* coordinator accepts the oldest supported coordination version
* coordinator accepts the newest supported coordination version
* coordinator rejects below-minimum and above-maximum coordination versions
* RedisStream starter emits the module-defined coordination version
* rolling-upgrade behavior when old and new members heartbeat against the same group

## Shared Protocol Artifact

Coordination version metadata와 기본 timing 값은 `redisstream-core`가 소유한다.
Coordinator server와 support module은 heartbeat interval, member lease TTL,
rebalance timeout, supported coordination version table을 각자 정의하지 않고 이 모듈을
의존해야 한다.

Coordination version `1`의 기본 timing contract는 다음과 같다.

* heartbeat interval: `3s`
* member lease TTL: `15s`
* rebalance timeout: `60s`

Coordinator는 heartbeat response에서 `heartbeatIntervalMs`와 `rebalanceTimeoutMs`를 내려주며, consumer는
join 이후 server response를 따라야 한다. Shared default는 첫 heartbeat가 성공하기 전에도
양쪽 artifact의 기본 동작이 일치하도록 하고, future protocol version에서 timing default를
한 곳에서 진화시키기 위한 계약이다.
