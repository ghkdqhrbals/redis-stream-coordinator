package io.github.ghkdqhrbals.redisstreamcoordinator.api

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatResponse
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.validation.Valid
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}")
@Tag(
    name = "Consumer Heartbeats",
    description = "Consumer membership, assignment reconciliation, revoke progress, and shard progress reporting.",
)
class MemberHeartbeatController(
    private val coordinator: CoordinatorService,
) {
    @Operation(
        operationId = "sendConsumerHeartbeat",
        summary = "Send consumer heartbeat",
        description = "Accepts one consumer heartbeat and returns the coordinator's current assignment view. Consumers use this endpoint to join, renew member leases, report owned/revoking shards, and receive revoke or metadata-sync instructions.",
        responses = [
            ApiResponse(responseCode = "200", description = "Heartbeat was processed. Check the response status for OK, RETRY, SYNC_METADATA, or fencing states."),
            ApiResponse(responseCode = "400", description = "The heartbeat payload violates the coordinator contract."),
            ApiResponse(responseCode = "404", description = "The group does not exist."),
        ],
    )
    @PostMapping("/heartbeat")
    fun heartbeat(
        @Parameter(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
        @PathVariable streamPrefix: String,
        @Parameter(description = "Redis Stream consumer group name.", example = "demo-workers")
        @PathVariable consumerGroup: String,
        @Parameter(description = "Coordinator member id carried by the consumer.", example = "consumer-pod-2")
        @PathVariable memberId: String,
        @Valid @RequestBody request: HeartbeatRequest,
    ): HeartbeatResponse =
        coordinator.heartbeat(streamPrefix, consumerGroup, memberId, request)
}
