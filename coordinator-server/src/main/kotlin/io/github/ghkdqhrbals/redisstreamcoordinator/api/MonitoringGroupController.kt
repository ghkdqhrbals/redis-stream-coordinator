package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.AssignmentsResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ConsumptionProgressResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupsResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MembersResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MigrationsResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/monitoring")
@Tag(
    name = "Monitoring",
    description = "Read-only health, group, member, assignment, migration, shard, message, and Grafana dashboard APIs.",
)
class MonitoringGroupController(
    private val coordinator: CoordinatorService,
) {
    @Operation(
        operationId = "listCoordinatorGroups",
        summary = "List coordinator groups",
        description = "Returns every coordinator-owned stream group metadata snapshot.",
        responses = [ApiResponse(responseCode = "200", description = "Group list.")],
    )
    @GetMapping("/groups")
    fun listGroups(): GroupsResponse =
        coordinator.listGroups()

    @Operation(
        operationId = "getMonitoringGroup",
        summary = "Read group monitoring snapshot",
        description = "Returns one group metadata snapshot for monitoring tools.",
        responses = [
            ApiResponse(responseCode = "200", description = "Group metadata."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}")
    fun getGroup(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): GroupResponse =
        coordinator.getGroup(streamPrefix, consumerGroup)

    @Operation(
        operationId = "listGroupMembers",
        summary = "List group members",
        description = "Returns current and historical members for one group, including ACTIVE, STARTING, LEAVING, EXPIRED, and FENCED states.",
        responses = [
            ApiResponse(responseCode = "200", description = "Member list."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/members")
    fun listMembers(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): MembersResponse =
        coordinator.listMembers(streamPrefix, consumerGroup)

    @Operation(
        operationId = "getGroupAssignments",
        summary = "Read assignment state",
        description = "Returns target assignments, current assignments, revoke progress, and invariant violations for one group.",
        responses = [
            ApiResponse(responseCode = "200", description = "Assignment snapshot."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/assignments")
    fun assignments(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): AssignmentsResponse =
        coordinator.assignments(streamPrefix, consumerGroup)

    @Operation(
        operationId = "getConsumptionProgress",
        summary = "Read consumer progress",
        description = "Returns the latest delivered and acknowledged Redis Stream ids reported by consumers per shard.",
        responses = [
            ApiResponse(responseCode = "200", description = "Consumer progress snapshot."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/consumption")
    fun consumptionProgress(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): ConsumptionProgressResponse =
        coordinator.consumptionProgress(streamPrefix, consumerGroup)

    @Operation(
        operationId = "listReshardingMigrations",
        summary = "List resharding migrations",
        description = "Returns resharding history and active resharding state for one group.",
        responses = [
            ApiResponse(responseCode = "200", description = "Migration list."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/migrations")
    fun migrations(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
    ): MigrationsResponse =
        coordinator.migrations(streamPrefix, consumerGroup)
}
