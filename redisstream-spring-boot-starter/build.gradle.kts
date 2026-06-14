plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
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
    compilerOptions {
        freeCompilerArgs.add("-Xannotation-default-target=param-property")
    }
}

dependencies {
    api(platform(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES))
    api(project(":redisstream-core"))

    api("org.springframework.boot:spring-boot-autoconfigure")
    api("org.springframework.data:spring-data-redis")
    api("org.springframework:spring-context")
    api("org.springframework:spring-web")

    implementation("tools.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
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
        artifactId = "redisstream-spring-boot-starter",
        version = project.version.toString(),
    )

    pom {
        name.set("Redis Stream Coordinator Spring Boot Starter")
        description.set("Spring Boot starter for Redis Stream Coordinator consumers and named StreamProducer publishers.")
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
