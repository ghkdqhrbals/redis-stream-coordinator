package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
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
    @PostMapping
    fun createGroup(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: CreateGroupRequest,
    ): ResponseEntity<GroupResponse> =
        ResponseEntity.status(HttpStatus.CREATED).body(
            coordinator.createGroup(streamPrefix, consumerGroup, request),
        )

    @GetMapping
    fun getGroup(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): GroupResponse =
        coordinator.getGroup(streamPrefix, consumerGroup)

    @GetMapping("/producer-routing")
    fun getProducerRouting(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): ProducerRoutingResponse =
        coordinator.producerRouting(streamPrefix, consumerGroup)

    @PostMapping("/scale")
    fun scaleGroup(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: ScaleGroupRequest,
    ): ResponseEntity<Migration> =
        ResponseEntity.status(HttpStatus.ACCEPTED).body(
            coordinator.scaleGroup(streamPrefix, consumerGroup, request),
        )

    @PatchMapping("/consumer-concurrency")
    fun updateConsumerConcurrency(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @Valid @RequestBody request: UpdateConsumerConcurrencyRequest,
    ): ConsumerConcurrencyResponse =
        coordinator.updateConsumerConcurrency(streamPrefix, consumerGroup, request)

    @GetMapping("/migrations/{reshardingId}")
    fun getMigration(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @PathVariable reshardingId: String,
    ): Migration =
        coordinator.getMigration(streamPrefix, consumerGroup, reshardingId)

    @PostMapping("/migrations/{reshardingId}/rollback")
    fun rollbackMigration(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
        @PathVariable reshardingId: String,
        @Valid @RequestBody request: RollbackMigrationRequest,
    ): ResponseEntity<Migration> =
        ResponseEntity.status(HttpStatus.ACCEPTED).body(
            coordinator.rollbackMigration(streamPrefix, consumerGroup, reshardingId),
        )
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

    @GetMapping("/streams/{streamPrefix}/groups/{consumerGroup}/migrations")
    fun migrations(
        @PathVariable streamPrefix: String,
        @PathVariable consumerGroup: String,
    ): MigrationsResponse =
        coordinator.migrations(streamPrefix, consumerGroup)
}
