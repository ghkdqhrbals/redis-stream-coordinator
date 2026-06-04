package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamMessagePageDirection
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamMessagesPageResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamShardOffsetsResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/monitoring")
@Tag(
    name = "Monitoring",
    description = "Read-only health, group, member, assignment, migration, shard, message, and Grafana dashboard APIs.",
)
class MonitoringStreamController(
    private val coordinator: CoordinatorService,
) {
    @Operation(
        operationId = "getStreamShardOffsets",
        summary = "Read shard offsets and lag",
        description = "Returns per-shard stream length, Redis consumer-group lag, pending count, memory usage, Redis Cluster node placement, and live owner members.",
        responses = [
            ApiResponse(responseCode = "200", description = "Shard offset snapshot."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/offsets")
    fun offsets(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): StreamShardOffsetsResponse =
        coordinator.streamShardOffsets(streamPrefix, consumerGroup)

    @Operation(
        operationId = "getStreamShardMessages",
        summary = "Read shard messages",
        description = "Reads a cursor-based page of Redis Stream messages for one shard without changing consumer group state.",
        responses = [
            ApiResponse(responseCode = "200", description = "Message page."),
            ApiResponse(responseCode = "404", description = "The group or shard does not exist."),
        ],
    )
    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/shards/{shardIndex}/messages")
    fun messages(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
        @Parameter(description = "Shard index.", example = "4")
        @PathVariable shardIndex: Int,
        @Parameter(description = "Pagination direction. BACKWARD reads newest records first; FORWARD reads older-to-newer from the cursor.", example = "BACKWARD")
        @RequestParam(defaultValue = "BACKWARD") direction: StreamMessagePageDirection,
        @Parameter(description = "Cursor returned by the previous page. Omit for the first page.")
        @RequestParam(required = false) cursor: String?,
        @Parameter(description = "Maximum records to return.", example = "25")
        @RequestParam(defaultValue = "25") limit: Int,
    ): StreamMessagesPageResponse =
        coordinator.streamMessages(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            shardIndex = shardIndex,
            direction = direction,
            cursor = cursor,
            limit = limit,
        )
}
