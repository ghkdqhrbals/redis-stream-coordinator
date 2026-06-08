plugins {
    kotlin("jvm")
    `java-library`
    alias(libs.plugins.maven.publish)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(24)
    }
}

kotlin {
    jvmToolchain(24)
}

dependencies {
    testImplementation(kotlin("test"))
}

tasks.withType<Test> {
    useJUnitPlatform()
}

mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
    if (providers.gradleProperty("signingInMemoryKey").isPresent) {
        signAllPublications()
    }

    coordinates(
        groupId = project.group.toString(),
        artifactId = "redisstream-core",
        version = project.version.toString(),
    )

    pom {
        name.set("Redis Stream Coordinator Core")
        description.set("Shared coordination protocol contracts and version defaults for Redis Stream Coordinator.")
        inceptionYear.set("2026")
        url.set("https://github.com/ghkdqhrbals/redis-stream-coordinator")
        licenses {
            license {
                name.set("Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("ghkdqhrbals")
                name.set("ghkdqhrbals")
                url.set("https://github.com/ghkdqhrbals")
            }
        }
        scm {
            url.set("https://github.com/ghkdqhrbals/redis-stream-coordinator")
            connection.set("scm:git:https://github.com/ghkdqhrbals/redis-stream-coordinator.git")
            developerConnection.set("scm:git:ssh://git@github.com/ghkdqhrbals/redis-stream-coordinator.git")
        }
    }
}
