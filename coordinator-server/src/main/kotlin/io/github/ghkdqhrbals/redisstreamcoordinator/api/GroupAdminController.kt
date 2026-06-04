package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Hidden
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.DeleteGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}")
@Tag(
    name = "Group Administration",
    description = "Create, read, and delete coordinator-owned stream group metadata.",
)
class GroupAdminController(
    private val coordinator: CoordinatorService,
) {
    /**
     * Creates a coordinator-owned group metadata record and initial shard layout.
     */
    @Operation(
        operationId = "createCoordinatorGroup",
        summary = "Create a stream group",
        description = "Creates the coordinator source-of-truth metadata and initial shard layout for a sharded Redis Stream group. Producers and consumers fail fast when this group does not exist.",
        responses = [
            ApiResponse(responseCode = "201", description = "Group metadata was created."),
            ApiResponse(responseCode = "409", description = "The group already exists."),
            ApiResponse(responseCode = "503", description = "Redis or stream provisioning is unavailable."),
        ],
    )
    @Hidden
    @PostMapping
    fun createGroup(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: CreateGroupRequest,
    ): ResponseEntity<GroupResponse> {
        val response = coordinator.createGroup(streamPrefix, consumerGroup, request)
        return ResponseEntity.status(HttpStatus.CREATED).body(response)
    }

    /**
     * Returns the current coordinator source-of-truth metadata for a group.
     */
    @Operation(
        operationId = "getCoordinatorGroup",
        summary = "Read group metadata",
        description = "Returns the current coordinator metadata for one stream group, including epochs, shard count, active resharding state, and assignment summaries.",
        responses = [
            ApiResponse(responseCode = "200", description = "Group metadata."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping
    fun getGroup(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): GroupResponse =
        coordinator.getGroup(streamPrefix, consumerGroup)

    /**
     * Deletes coordinator metadata for an inactive group.
     */
    @Operation(
        operationId = "deleteCoordinatorGroup",
        summary = "Delete group metadata",
        description = "Deletes coordinator metadata for an inactive group. Use force only for operational recovery when members cannot leave cleanly.",
        responses = [
            ApiResponse(responseCode = "200", description = "Deleted group metadata snapshot."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
            ApiResponse(responseCode = "422", description = "The group is not safe to delete without force."),
        ],
    )
    @DeleteMapping
    fun deleteGroup(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: DeleteGroupRequest,
    ): GroupResponse =
        coordinator.deleteGroup(streamPrefix, consumerGroup, request)
}
