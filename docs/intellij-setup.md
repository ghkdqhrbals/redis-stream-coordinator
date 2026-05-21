# IntelliJ Setup

This project uses Spring Boot 4, Kotlin 2.2, Gradle 8.14+, and Java 24.

## Required Build Mode

Use Gradle for build and test execution.

In IntelliJ IDEA:

1. Open the repository root, not the `coordinator-server` directory.
2. Go to `Settings > Build, Execution, Deployment > Build Tools > Gradle`.
3. Set `Build and run using` to `Gradle`.
4. Set `Run tests using` to `Gradle`.
5. Set `Gradle JVM` to a Java 24 JDK if available, or let Gradle use the configured Java toolchain.
6. Reimport the Gradle project.

## Kotlin JPS Warning

If IntelliJ shows this warning:

```text
Version (...) of the Kotlin JPS plugin will be used.
Kotlin JPS compiler maximum supported version is '2.1.0' but '2.2.21' is specified.
```

the IDE is trying to build with JPS instead of Gradle. Do not downgrade Kotlin runtime libraries. Spring Boot 4 requires Kotlin 2.2 or later.

Switch build execution to Gradle and sync again.
