package io.github.ghkdqhrbals.redisstreamcoordinator

import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import java.time.Instant
import java.util.UUID

enum class GroupState { EMPTY, ASSIGNING, RECONCILING, STABLE }
enum class MemberState { STARTING, ACTIVE, LEAVING, EXPIRED, FENCED }
enum class MigrationState { PREPARING, ACTIVE, DRAINING, DEPRECATED, ROLLING_BACK, ROLLED_BACK }
enum class HeartbeatStatus { OK, RETRY, UNKNOWN_MEMBER_ID, FENCED_MEMBER_EPOCH, UNSUPPORTED_PROTOCOL, INVALID_REQUEST }
enum class RevokingShardState { REVOKING, DRAINING, REVOKED }

data class GroupKey(
    val streamPrefix: String,
    val consumerGroup: String,
)

data class ShardId(
    val streamVersion: Int,
    val shardIndex: Int,
) : Comparable<ShardId> {
    override fun compareTo(other: ShardId): Int =
        compareValuesBy(this, other, ShardId::streamVersion, ShardId::shardIndex)
}

data class ConsumerConcurrencyPolicy(
    @field:Min(1)
    val defaultMaxConcurrency: Int,
    val memberOverrides: Map<String, Int> = emptyMap(),
)

data class CreateGroupRequest(
    @field:Min(1)
    val initialShardCount: Int? = null,
    @field:NotBlank
    val hashAlgorithm: String,
    val hashSeed: String = "default",
    val versionPolicy: String = "AUTO_INCREMENT",
    @field:Valid
    val consumerConcurrencyPolicy: ConsumerConcurrencyPolicy? = null,
    @field:NotBlank
    val requestedBy: String,
    val reason: String? = null,
)

data class ScaleGroupRequest(
    @field:Min(1)
    val targetShardCount: Int,
    @field:Valid
    val consumerConcurrencyPolicy: ConsumerConcurrencyPolicy? = null,
    @field:NotBlank
    val requestedBy: String,
    @field:NotBlank
    val reason: String,
    val deprecatedAfter: String? = null,
)

data class UpdateConsumerConcurrencyRequest(
    @field:Min(1)
    val defaultMaxConcurrency: Int,
    val memberOverrides: Map<String, Int> = emptyMap(),
    @field:NotBlank
    val requestedBy: String,
    @field:NotBlank
    val reason: String,
)

data class RollbackMigrationRequest(
    @field:NotBlank
    val requestedBy: String,
    @field:NotBlank
    val reason: String,
)

data class RuntimeConsumerCapacity(
    @field:Min(1)
    val runtimeMaxConcurrency: Int,
    @field:Min(0)
    val availableConcurrency: Int,
)

data class RevokingShardReport(
    val shard: ShardId,
    val state: RevokingShardState,
    val inFlight: Int = 0,
    val ackedAt: Instant? = null,
)

data class HeartbeatRequest(
    @field:Min(1)
    val protocolVersion: Int,
    @field:NotBlank
    val requestId: String,
    @field:NotBlank
    val memberId: String,
    val memberName: String? = null,
    val memberEpoch: Long,
    val rebalanceTimeoutMs: Long? = null,
    val metadataVersion: Long,
    @field:Valid
    val runtimeConsumerCapacity: RuntimeConsumerCapacity,
    val ownedShards: Set<ShardId> = emptySet(),
    val revokingShards: List<RevokingShardReport> = emptyList(),
)

data class AssignmentView(
    val assignedShards: Set<ShardId>,
    val pendingShards: Set<ShardId>,
    val metadataVersion: Long,
)

data class HeartbeatResponse(
    val responseTo: String,
    val status: HeartbeatStatus,
    val memberId: String,
    val memberEpoch: Long,
    val heartbeatIntervalMs: Long,
    val groupEpoch: Long,
    val assignmentEpoch: Long,
    val metadataVersion: Long,
    val assignedMaxConcurrency: Int,
    val assignment: AssignmentView,
)

data class Migration(
    val migrationId: String,
    val fromVersion: Int,
    val toVersion: Int,
    val fromShardCount: Int,
    val toShardCount: Int,
    var state: MigrationState,
    val createdAt: Instant,
    var updatedAt: Instant,
)

data class MemberMetadata(
    val memberId: String,
    var memberName: String,
    var state: MemberState,
    var memberEpoch: Long,
    var metadataVersion: Long,
    var assignedMaxConcurrency: Int,
    var runtimeMaxConcurrency: Int,
    var activeConsumerWorkers: Int,
    var currentAssignment: Set<ShardId>,
    var revoking: Set<ShardId>,
    var lastHeartbeatAt: Instant,
    var memberLeaseExpiresAt: Instant,
    var rebalanceTimeoutMs: Long = 60_000,
    var rebalanceDeadlineAt: Instant? = null,
)

data class GroupMetadata(
    val streamPrefix: String,
    val consumerGroup: String,
    var storeRevision: Long = 0,
    var groupEpoch: Long,
    var metadataVersion: Long,
    var assignmentEpoch: Long,
    var state: GroupState,
    var activeWriteVersion: Int,
    var readableVersions: Set<Int>,
    var shardCountsByVersion: MutableMap<Int, Int>,
    val hashAlgorithm: String,
    val hashSeed: String,
    var consumerConcurrencyPolicy: ConsumerConcurrencyPolicy,
    val members: MutableMap<String, MemberMetadata> = linkedMapOf(),
    var targetAssignments: MutableMap<String, MutableSet<ShardId>> = linkedMapOf(),
    var migrations: MutableMap<String, Migration> = linkedMapOf(),
    var activeMigrationId: String? = null,
    val createdAt: Instant,
    var updatedAt: Instant,
)

data class GroupResponse(
    val streamPrefix: String,
    val consumerGroup: String,
    val state: GroupState,
    val groupEpoch: Long,
    val assignmentEpoch: Long,
    val metadataVersion: Long,
    val activeWriteVersion: Int,
    val readableVersions: Set<Int>,
    val shardCount: Int,
    val consumerConcurrencyPolicy: ConsumerConcurrencyPolicy,
    val activeMigration: Migration?,
    val targetAssignmentSummary: Map<String, Int>,
    val currentAssignmentSummary: Map<String, Int>,
)

data class ConsumerConcurrencyResponse(
    val streamPrefix: String,
    val consumerGroup: String,
    val metadataVersion: Long,
    val groupEpoch: Long,
    val consumerConcurrencyPolicy: ConsumerConcurrencyPolicy,
    val affectedMembers: List<String>,
)

data class HealthResponse(
    val status: String,
    val coordinatorId: String,
    val redis: String,
    val loop: String,
)

data class GroupsResponse(val groups: List<GroupResponse>)
data class MembersResponse(val members: List<MemberMetadata>)
data class AssignmentsResponse(
    val targetAssignment: Map<String, Set<ShardId>>,
    val currentAssignments: Map<String, Set<ShardId>>,
    val revokeProgress: Map<String, Set<ShardId>>,
    val invariantViolations: List<String>,
)
data class MigrationsResponse(
    val migrations: List<Migration>,
    val activeMigration: String?,
)

fun newMigrationId(): String = "mig-${UUID.randomUUID()}"
