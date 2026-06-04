package io.github.ghkdqhrbals.redisstreamcoordinator

import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.readText
import kotlin.test.assertTrue

class CoordinatorOpenApiOperationIdContractTest {
    private val repoRoot: Path = run {
        val workingDir = Path.of(System.getProperty("user.dir"))
        if (Files.isRegularFile(workingDir.resolve("docs/openapi/coordinator.v1.yaml"))) {
            workingDir
        } else {
            workingDir.parent
        }
    }

    @Test
    fun `openapi operation IDs and controller annotations stay in sync`() {
        val specIds = operationIdsFromSpec()
        val controllerIds = operationIdsFromControllers()

        val missingInControllers = specIds - controllerIds
        val extraInControllers = controllerIds - specIds

        val issues = buildList {
            if (missingInControllers.isNotEmpty()) {
                add("Missing controller annotation(s): ${missingInControllers.sorted().joinToString(", ")}")
            }

            if (extraInControllers.isNotEmpty()) {
                add("Missing OpenAPI entry(ies): ${extraInControllers.sorted().joinToString(", ")}")
            }
        }

        assertTrue(
            issues.isEmpty(),
            issues.joinToString(prefix = "OperationId contract drift detected: ", separator = "; "),
        )
    }

    private fun operationIdsFromSpec(): Set<String> {
        val specPath = repoRoot.resolve("docs/openapi/coordinator.v1.yaml")
        val specContent = specPath.readText()
        return Regex("operationId:\\s+([A-Za-z0-9_]+)")
            .findAll(specContent)
            .map { it.groupValues[1] }
            .toSet()
    }

    private fun operationIdsFromControllers(): Set<String> {
        val controllerRoot: Path = repoRoot.resolve("coordinator-server/src/main/kotlin/io/github/ghkdqhrbals/redisstreamcoordinator/api")
        val files = Files.walk(controllerRoot).use { stream ->
            stream.filter { it.fileName.toString().endsWith("Controller.kt") }
                .toList()
        }

        return files
            .flatMap { file ->
                val source = file.readText()
                Regex("operationId\\s*=\\s*\"([A-Za-z0-9_]+)\"")
                    .findAll(source)
                    .map { it.groupValues[1] }
                    .toList()
            }
            .toSet()
    }
}
