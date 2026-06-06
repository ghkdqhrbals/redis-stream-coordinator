package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateStreamRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ScaleStreamRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamCreateResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamScaleResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/streams/{streamPrefix}")
@Tag(
    name = "Stream Operations",
    description = "Stream-level shard layout operations that fan out to all consumer groups of the stream.",
)
class StreamOperationsController(
    private val coordinator: CoordinatorService,
) {
    /**
     * Creates a stream-level shard layout with only a stream prefix in the path.
     */
    @Operation(
        operationId = "createStreamGroup",
        summary = "Create a stream group",
        description = "Creates the initial shard layout for a sharded Redis Stream prefix. The official create path only requires streamPrefix; consumer groups reconcile through their own runtime configuration and heartbeat flow.",
        responses = [
            ApiResponse(responseCode = "201", description = "Stream shard layout was created."),
            ApiResponse(responseCode = "409", description = "The stream prefix already has coordinator metadata or Redis Stream keys."),
            ApiResponse(responseCode = "503", description = "Redis or stream provisioning is unavailable."),
        ],
    )
    @PostMapping
    fun createStream(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Valid @RequestBody request: CreateStreamRequest,
    ): ResponseEntity<StreamCreateResponse> =
        ResponseEntity.status(HttpStatus.CREATED).body(coordinator.createStream(streamPrefix, request))

    /**
     * Changes the physical shard count for a stream prefix across every registered consumer group.
     */
    @Operation(
        operationId = "scaleStreamShards",
        summary = "Start shard scale-out or scale-in",
        description = "Changes the shard count for the stream prefix. Consumer groups do not appear in the request path; each registered group observes the new shard set on heartbeat and reconciles assignment independently. During scale-in, removed shard streams are retired only after live owners release them or expire and Redis reports drained consumer groups.",
        responses = [
            ApiResponse(responseCode = "202", description = "Stream shard scale was accepted for affected consumer groups."),
            ApiResponse(responseCode = "404", description = "No coordinator groups exist for the stream prefix."),
            ApiResponse(responseCode = "409", description = "At least one affected consumer group already has an incompatible active resharding."),
        ],
    )
    @PostMapping("/scale")
    fun scaleStream(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Valid @RequestBody request: ScaleStreamRequest,
    ): ResponseEntity<StreamScaleResponse> =
        ResponseEntity.status(HttpStatus.ACCEPTED).body(coordinator.scaleStream(streamPrefix, request))
}
