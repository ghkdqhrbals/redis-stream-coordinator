# Changelog

All notable changes to this project are tracked here.

This project follows Semantic Versioning after `1.0.0`. Before `1.0.0`, compatibility-affecting changes are still documented because the coordinator protocol and Redis metadata are operational contracts.

## Unreleased

### Added

* Spring Boot coordinator server for Redis Stream shard metadata, member heartbeat, assignment, migration, monitoring, ACL, audit, metrics, Redis-backed state, and stream provisioning.
* `com.redisstream:redisstream-spring-boot-starter` for consumer heartbeat lifecycle, shard callbacks, runtime capacity reporting, Redis Stream polling, producer routing, and publishing.
* Producer routing cache invalidation after Redis Stream write failure, plus opt-in bounded publish retry for idempotent producers.
* Monitoring metrics for consumer ACK status, runtime capacity, producer publish attempts, routing invalidation, and coordinator state conflict retries.
* Dockerfile, local Compose coordinator profile, Docker smoke workflow, and manual GHCR publish workflow.
* PR test report rendering and Codex review workflow.

### Compatibility

* HTTP API prefix: `/coord/v1`.
* Heartbeat protocol version: `1`.
* Redis metadata schema version: `1`.
