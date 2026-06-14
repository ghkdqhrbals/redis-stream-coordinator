---
name: rsc-release-maven-publish
description: Release Redis Stream Coordinator JVM artifacts to Maven Central from a committed feature/coordinator state.
---

# RSC Release Maven Publish

Use this project-local skill before publishing `redisstream-core` or `redisstream-spring-boot-starter` to Maven Central.

## Scope

This repository publishes JVM artifacts only:

- `io.github.ghkdqhrbals:redisstream-core`
- `io.github.ghkdqhrbals:redisstream-spring-boot-starter`

Do not publish a Python package from this repository.

## Steps

1. Bump `gradle.properties` `projectVersion` to the requested patch/minor version.
2. Update `.github/workflows/maven-central.yml` default/example version.
3. Add concise English and Korean release notes under `docs/releases` and `docs/ko/releases`.
4. Run tests before publishing:

```sh
./gradlew test --no-daemon --rerun-tasks
git diff --check
```

5. Commit and push the release state to `feature/coordinator`.
6. Run the `Maven Central` GitHub Actions workflow with the exact version input.
7. Watch the workflow to completion and inspect logs if it fails.

## Rules

- Never print Maven Central credentials, signing keys, Redis passwords, or coordinator passwords.
- The workflow validates that `gradle.properties` matches the manual input version.
- Maven Central publishing is complete only after the GitHub Actions run succeeds.
