plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.spring) apply false
    alias(libs.plugins.spring.boot) apply false
}

tasks.named<org.gradle.api.tasks.wrapper.Wrapper>("wrapper") {
    gradleVersion = "8.14.5"
    distributionType = org.gradle.api.tasks.wrapper.Wrapper.DistributionType.BIN
}

fun org.gradle.api.Project.ensurePrepareKotlinBuildScriptModelTask() {
    if (tasks.findByName("prepareKotlinBuildScriptModel") == null) {
        tasks.register("prepareKotlinBuildScriptModel") {
            group = "ide"
            description = "Compatibility task for IDE Gradle Kotlin DSL model import."
        }
    }
}

ensurePrepareKotlinBuildScriptModelTask()

subprojects {
    group = "io.github.ghkdqhrbals"
    version = providers.gradleProperty("projectVersion").get()

    tasks.register("wrapper") {
        group = "Build Setup"
        description = "Runs the root Gradle wrapper task."
        dependsOn(rootProject.tasks.named("wrapper"))
    }

    ensurePrepareKotlinBuildScriptModelTask()
}
