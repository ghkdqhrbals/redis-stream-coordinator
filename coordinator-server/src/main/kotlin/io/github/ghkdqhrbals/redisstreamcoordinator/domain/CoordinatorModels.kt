package io.github.ghkdqhrbals.redisstreamcoordinator.domain

import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Instant
import java.util.UUID

enum class GroupState {
    /** The group has no active members or assignable ownership yet. */
    EMPTY,

    /** The coordinator is calculating or installing target shard assignments. */
    ASSIGNING,

    /** Members are converging current ownership toward the coordinator target assignment. */
    RECONCILING,

    /** Target and current assignments have converged for all live members. */
    STABLE,
}
enum class MemberState { STARTING, ACTIVE, LEAVING, EXPIRED, FENCED }
enum class MigrationState {
    /** The next stream version is being prepared before writes move to it. */
    PREPARING,

    /** Producer writes use the new stream version while consumers may still read old and new versions. */
    ACTIVE,

    /** The old stream version is no longer written and is waiting for consumers to drain. */
    DRAINING,

    /** The old stream version is no longer part of normal reads or writes. */
    DEPRECATED,

    /** The migration is reverting writes and reads back to the previous stream version. */
    ROLLING_BACK,

    /** Rollback has completed and the previous stream version is authoritative again. */
    ROLLED_BACK,
}
enum class HeartbeatStatus {
    /** The heartbeat was accepted and the response contains the current assignment view. */
    OK,

    /** The member should retry because the coordinator could not safely process the heartbeat now. */
    RETRY,

    /** The member must replace its local metadata view with the coordinator response metadata. */
    SYNC_METADATA,

    /** The member metadata version is correct, but revoke-before-assign handoff is still draining. */
    REVOKE_PENDING,

    /** The coordinator does not know this member id for the requested group. */
    UNKNOWN_MEMBER_ID,

    /** The member epoch is stale or fenced and the member must stop local work before rejoining. */
    FENCED_MEMBER_EPOCH,

    /** The request uses a coordination protocol version outside the supported range. */
    UNSUPPORTED_PROTOCOL,

    /** The heartbeat request is malformed or violates the coordinator contract. */
    INVALID_REQUEST,
}
enum class RevokingShardState { REVOKING, DRAINING, REVOKED }

const val COORDINATOR_METADATA_SCHEMA_VERSION = 1

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

data class ShardConsumptionProgress(
    val shard: ShardId,
    val streamKey: String,
    val lastDeliveredId: String? = null,
    val lastAckedId: String? = null,
    val pendingCount: Long = 0,
    val updatedAt: Instant? = null,
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
    val shardProgress: List<ShardConsumptionProgress> = emptyList(),
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
    @param:JsonAlias("migrationId")
    @field:JsonAlias("migrationId")
    val reshardingId: String,
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
    var shardProgress: List<ShardConsumptionProgress> = emptyList(),
)

data class MetadataCorrection(
    val targetMetadataVersion: Long,
    var observedMetadataVersion: Long,
    val requestedAt: Instant,
    var updatedAt: Instant,
    val acknowledgedMembers: MutableSet<String> = linkedSetOf(),
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class GroupMetadata(
    val streamPrefix: String,
    val consumerGroup: String,
    var schemaVersion: Int = COORDINATOR_METADATA_SCHEMA_VERSION,
    var storeRevision: Long = 0,
    var groupEpoch: Long,
    var metadataVersion: Long,
    var assignmentEpoch: Long,
    var state: GroupState,
    var activeWriteVersion: Int,
    var readableVersions: Set<Int>,
    var shardCountsByVersion: MutableMap<Int, Int>,
    var consumerConcurrencyPolicy: ConsumerConcurrencyPolicy,
    val members: MutableMap<String, MemberMetadata> = linkedMapOf(),
    var targetAssignments: MutableMap<String, MutableSet<ShardId>> = linkedMapOf(),
    var migrations: MutableMap<String, Migration> = linkedMapOf(),
    @param:JsonAlias("activeMigrationId")
    @field:JsonAlias("activeMigrationId")
    var activeReshardingId: String? = null,
    var metadataCorrection: MetadataCorrection? = null,
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

data class ProducerRoutingShard(
    val streamVersion: Int,
    val shardIndex: Int,
    val streamKey: String,
    val redisSlot: Int,
)

data class ProducerRoutingResponse(
    val streamPrefix: String,
    val consumerGroup: String,
    val metadataVersion: Long,
    val activeWriteVersion: Int,
    val shardCount: Int,
    val streamKeyPattern: String,
    val shards: List<ProducerRoutingShard>,
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
    val activeReshardingId: String?,
)

data class ConsumerShardProgress(
    val streamPrefix: String,
    val consumerGroup: String,
    val memberId: String,
    val memberName: String,
    val memberState: MemberState,
    val shard: ShardId,
    val streamKey: String,
    val lastDeliveredId: String?,
    val lastAckedId: String?,
    val pendingCount: Long,
    val updatedAt: Instant?,
)
data class ConsumptionProgressResponse(val progress: List<ConsumerShardProgress>)

fun newReshardingId(): String = "reshard-${UUID.randomUUID()}"
