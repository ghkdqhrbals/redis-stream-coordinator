package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Hidden
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.Migration
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ProducerRoutingResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RollbackMigrationRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ScaleGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}")
@Tag(
    name = "Group Operations",
    description = "Producer routing, shard resharding, and migration rollback operations.",
)
class GroupOperationsController(
    private val coordinator: CoordinatorService,
) {
    /**
     * Returns read-only producer routing metadata for the current shard layout.
     */
    @Operation(
        operationId = "getProducerRoutingMetadata",
        summary = "Read producer routing metadata",
        description = "Returns the shard count, stream key pattern, concrete shard keys, and Redis Cluster slots that producers must use when routing partition keys.",
        responses = [
            ApiResponse(responseCode = "200", description = "Producer routing metadata."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping("/producer-routing")
    fun getProducerRouting(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): ProducerRoutingResponse =
        coordinator.producerRouting(streamPrefix, consumerGroup)

    /**
     * Starts shard scale-out or scale-in.
     */
    @Operation(
        operationId = "startGroupResharding",
        summary = "Start shard scale-out or scale-in",
        description = "Creates a coordinator-managed resharding operation. The coordinator provisions shard stream keys and reconciles consumer ownership through heartbeat responses. During scale-in, if every live member expires, the coordinator skips consumer-level revoke wait and completes only after Redis reports removed shard consumer groups are drained.",
        responses = [
            ApiResponse(responseCode = "202", description = "Resharding was accepted."),
            ApiResponse(responseCode = "409", description = "Another migration is already active."),
            ApiResponse(responseCode = "503", description = "Redis stream provisioning failed."),
        ],
    )
    @Hidden
    @PostMapping("/scale")
    fun scaleGroup(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: ScaleGroupRequest,
    ): ResponseEntity<Migration> {
        val migration = coordinator.scaleGroup(streamPrefix, consumerGroup, request)
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(migration)
    }

    /**
     * Returns one recorded resharding migration by id.
     */
    @Operation(
        operationId = "getReshardingMigration",
        summary = "Read one resharding migration",
        description = "Returns one recorded resharding operation, including source/target shard counts, state, and timestamps.",
        responses = [
            ApiResponse(responseCode = "200", description = "Resharding migration metadata."),
            ApiResponse(responseCode = "404", description = "The migration does not exist."),
        ],
    )
    @GetMapping("/migrations/{reshardingId}")
    fun getMigration(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
        @Parameter(description = "Coordinator-generated resharding id.", example = "reshard-00000000-0000-0000-0000-000000000000")
        @PathVariable reshardingId: String,
    ): Migration =
        coordinator.getMigration(streamPrefix, consumerGroup, reshardingId)

    /**
     * Requests rollback of an active migration when rollback is still allowed.
     */
    @Operation(
        operationId = "rollbackReshardingMigration",
        summary = "Rollback an active resharding migration",
        description = "Requests rollback of an active resharding operation while rollback is still allowed by the coordinator state machine.",
        responses = [
            ApiResponse(responseCode = "202", description = "Rollback was accepted."),
            ApiResponse(responseCode = "404", description = "The migration does not exist."),
            ApiResponse(responseCode = "422", description = "Rollback is not allowed in the current migration state."),
        ],
    )
    @PostMapping("/migrations/{reshardingId}/rollback")
    fun rollbackMigration(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
        @Parameter(description = "Coordinator-generated resharding id.", example = "reshard-00000000-0000-0000-0000-000000000000")
        @PathVariable reshardingId: String,
        @Valid @RequestBody request: RollbackMigrationRequest,
    ): ResponseEntity<Migration> {
        val migration = coordinator.rollbackMigration(streamPrefix, consumerGroup, reshardingId)
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(migration)
    }
}
