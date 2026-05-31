package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.protocol.CoordinatorCompatibilityResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}")
class AdminController(
    private val coordinator: CoordinatorService,
) {
    /**
     * Creates a coordinator-owned group metadata record and initial stream version.
     */
    @PostMapping
    fun createGroup(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: CreateGroupRequest,
    ): ResponseEntity<GroupResponse> {
        // The service validates duplicate groups, stores the initial metadata, and provisions stream shards when enabled.
        val response = coordinator.createGroup(streamPrefix, consumerGroup, request)
        return ResponseEntity.status(HttpStatus.CREATED).body(response)
    }

    /**
     * Returns the current coordinator source-of-truth metadata for a group.
     */
    @GetMapping
    fun getGroup(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): GroupResponse {
        // Reads the latest group metadata and applies any time-based state refresh before returning.
        return coordinator.getGroup(streamPrefix, consumerGroup)
    }

    /**
     * Returns read-only producer routing metadata for the active write stream version.
     */
    @GetMapping("/producer-routing")
    fun getProducerRouting(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): ProducerRoutingResponse {
        // Producers use this response as a cacheable routing snapshot; this endpoint does not mutate shard layout.
        return coordinator.producerRouting(streamPrefix, consumerGroup)
    }

    /**
     * Starts shard scale-out or scale-in by creating the next stream-version migration.
     */
    @PostMapping("/scale")
    fun scaleGroup(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: ScaleGroupRequest,
    ): ResponseEntity<Migration> {
        // The service records the migration first, then provisioning and rebalance continue from recorded metadata.
        val migration = coordinator.scaleGroup(streamPrefix, consumerGroup, request)
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(migration)
    }

    /**
     * Updates the server-side consumer worker capacity policy without changing shard count.
     */
    @PatchMapping("/consumer-concurrency")
    fun updateConsumerConcurrency(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: UpdateConsumerConcurrencyRequest,
    ): ConsumerConcurrencyResponse {
        // Assignment weights are recalculated only when the stored policy changes.
        return coordinator.updateConsumerConcurrency(streamPrefix, consumerGroup, request)
    }

    /**
     * Returns one recorded resharding migration by id.
     */
    @GetMapping("/migrations/{reshardingId}")
    fun getMigration(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @PathVariable reshardingId: String,
    ): Migration {
        // Migration state is read from coordinator metadata; Redis Stream data-plane records are not inspected here.
        return coordinator.getMigration(streamPrefix, consumerGroup, reshardingId)
    }

    /**
     * Requests rollback of an active migration when rollback is still allowed.
     */
    @PostMapping("/migrations/{reshardingId}/rollback")
    fun rollbackMigration(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @PathVariable reshardingId: String,
        @Valid @RequestBody request: RollbackMigrationRequest,
    ): ResponseEntity<Migration> {
        // The request body is validated at the API boundary; the service enforces rollback state and version rules.
        val migration = coordinator.rollbackMigration(streamPrefix, consumerGroup, reshardingId)
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(migration)
    }
}

@RestController
@RequestMapping("/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}")
class MemberController(
    private val coordinator: CoordinatorService,
) {
    @PostMapping("/heartbeat")
    fun heartbeat(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @PathVariable memberId: String,
        @Valid @RequestBody request: HeartbeatRequest,
    ): HeartbeatResponse =
        coordinator.heartbeat(streamPrefix, consumerGroup, memberId, request)
}

@RestController
@RequestMapping("/coord/v1/monitoring")
class MonitoringController(
    private val coordinator: CoordinatorService,
) {
    @GetMapping("/health")
    fun health(): HealthResponse =
        coordinator.health()

    @GetMapping("/compatibility")
    fun compatibility(): CoordinatorCompatibilityResponse =
        coordinator.compatibility()

    @GetMapping("/groups")
    fun listGroups(): GroupsResponse =
        coordinator.listGroups()

    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}")
    fun getGroup(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): GroupResponse =
        coordinator.getGroup(streamPrefix, consumerGroup)

    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/members")
    fun listMembers(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): MembersResponse =
        coordinator.listMembers(streamPrefix, consumerGroup)

    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/assignments")
    fun assignments(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): AssignmentsResponse =
        coordinator.assignments(streamPrefix, consumerGroup)

    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/consumption")
    fun consumptionProgress(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): ConsumptionProgressResponse =
        coordinator.consumptionProgress(streamPrefix, consumerGroup)

    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/migrations")
    fun migrations(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): MigrationsResponse =
        coordinator.migrations(streamPrefix, consumerGroup)
}
