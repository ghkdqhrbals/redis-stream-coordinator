package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GrafanaAssignmentRow
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GrafanaGroupRow
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GrafanaMemberRow
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GrafanaMessageRow
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GrafanaOptionRow
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GrafanaShardRow
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.StreamMessagePageDirection
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/monitoring/grafana")
@Tag(
    name = "Monitoring",
    description = "Read-only health, group, member, assignment, migration, shard, message, and Grafana dashboard APIs.",
)
class GrafanaMonitoringController(
    private val coordinator: CoordinatorService,
) {
    @Operation(
        operationId = "listGrafanaGroups",
        summary = "List Grafana group rows",
        description = "Returns one flat row per stream group for overview dashboard panels.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana group rows.")],
    )
    @GetMapping("/groups")
    fun groups(): List<GrafanaGroupRow> =
        coordinator.grafanaGroups()

    @Operation(
        operationId = "listGrafanaStreamOptions",
        summary = "List stream variable options",
        description = "Returns stream prefixes currently registered in coordinator metadata for Grafana variables.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana variable options.")],
    )
    @GetMapping("/options/streams")
    fun streamOptions(): List<GrafanaOptionRow> =
        coordinator.grafanaStreamOptions()

    @Operation(
        operationId = "listGrafanaConsumerGroupOptions",
        summary = "List consumer group variable options",
        description = "Returns consumer groups, optionally filtered by stream prefix, for chained Grafana variables.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana variable options.")],
    )
    @GetMapping("/options/consumer-groups")
    fun consumerGroupOptions(
        @Parameter(description = "Optional stream prefix filter.", example = "create-order")
        @RequestParam(required = false) streamPrefix: String?,
    ): List<GrafanaOptionRow> =
        coordinator.grafanaConsumerGroupOptions(streamPrefix)

    @Operation(
        operationId = "listGrafanaShardOptions",
        summary = "List shard variable options",
        description = "Returns shard indexes for one selected stream group.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana variable options.")],
    )
    @GetMapping("/options/shards")
    fun shardOptions(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @RequestParam streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @RequestParam consumerGroup: String,
    ): List<GrafanaOptionRow> =
        coordinator.grafanaShardOptions(streamPrefix, consumerGroup)

    @Operation(
        operationId = "listGrafanaMemberRows",
        summary = "List Grafana member rows",
        description = "Returns one flat row per consumer member for Grafana tables.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana member rows.")],
    )
    @GetMapping("/members")
    fun members(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @RequestParam streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @RequestParam consumerGroup: String,
    ): List<GrafanaMemberRow> =
        coordinator.grafanaMembers(streamPrefix, consumerGroup)

    @Operation(
        operationId = "listGrafanaShardRows",
        summary = "List Grafana shard rows",
        description = "Returns flat shard offset, lag, memory, and live owner rows for Grafana panels. Blank stream and group values scan all coordinator groups.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana shard rows.")],
    )
    @GetMapping("/shards")
    fun shards(
        @Parameter(description = "Sharded Redis Stream prefix, or blank to scan all groups.", example = "create-order")
        @RequestParam streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name, or blank to scan all groups.", example = "demo-workers")
        @RequestParam consumerGroup: String,
    ): List<GrafanaShardRow> =
        coordinator.grafanaShards(streamPrefix, consumerGroup)

    @Operation(
        operationId = "listGrafanaAssignmentRows",
        summary = "List Grafana assignment rows",
        description = "Returns flat target/current/revoking ownership rows per shard.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana assignment rows.")],
    )
    @GetMapping("/assignments")
    fun assignments(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @RequestParam streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @RequestParam consumerGroup: String,
    ): List<GrafanaAssignmentRow> =
        coordinator.grafanaAssignments(streamPrefix, consumerGroup)

    @Operation(
        operationId = "listGrafanaMessageRows",
        summary = "List Grafana message rows",
        description = "Returns flattened Redis Stream message rows for Grafana table panels. Use shardIndex=all to merge records from all shards.",
        responses = [ApiResponse(responseCode = "200", description = "Grafana message rows.")],
    )
    @GetMapping("/messages")
    fun messages(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @RequestParam streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @RequestParam consumerGroup: String,
        @Parameter(description = "Shard index or all.", example = "all")
        @RequestParam(defaultValue = "0") shardIndex: String,
        @Parameter(description = "Pagination direction.", example = "BACKWARD")
        @RequestParam(defaultValue = "BACKWARD") direction: StreamMessagePageDirection,
        @Parameter(description = "Cursor returned from the previous page.")
        @RequestParam(required = false) cursor: String?,
        @Parameter(description = "Exact Redis Stream record id to find across all shards. When present, cursor and direction are ignored.", example = "1780314922366-0")
        @RequestParam(required = false) recordId: String?,
        @Parameter(description = "Maximum records to return.", example = "25")
        @RequestParam(defaultValue = "25") limit: Int,
    ): List<GrafanaMessageRow> =
        coordinator.grafanaMessages(
            streamPrefix = streamPrefix,
            consumerGroup = consumerGroup,
            shardIndex = shardIndex,
            direction = direction,
            cursor = cursor,
            recordId = recordId,
            limit = limit,
        )
}
