package io.github.ghkdqhrbals.redisstreamcoordinator.domain

import com.redisstream.protocol.CoordinatorProtocol
import jakarta.validation.Valid
import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotBlank
import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.swagger.v3.oas.annotations.media.Schema
import java.time.Instant
import java.util.UUID

@Schema(description = "Coordinator group lifecycle state.")
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
@Schema(description = "Consumer member lifecycle state tracked by the coordinator.")
enum class MemberState { STARTING, ACTIVE, LEAVING, EXPIRED, FENCED }
@Schema(description = "Coordinator-managed resharding lifecycle state.")
enum class MigrationState {
    /** The target shard layout is being prepared before consumers receive it. */
    PREPARING,

    /** Producer routing uses the target shard count while consumers reconcile ownership. */
    ACTIVE,

    /** Removed shards are no longer assigned and consumers are draining revocations. */
    DRAINING,

    /** The resharding operation has completed. */
    DEPRECATED,

    /** The migration is reverting writes and reads back to the previous shard count. */
    ROLLING_BACK,

    /** Rollback has completed and the previous shard count is authoritative again. */
    ROLLED_BACK,
}
@Schema(description = "Result status returned by a member heartbeat.")
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
@Schema(description = "State reported by a consumer while it drains revoked shards.")
enum class RevokingShardState { REVOKING, DRAINING, REVOKED }

const val COORDINATOR_METADATA_SCHEMA_VERSION = 1

@Schema(description = "Coordinator group key for one sharded Redis Stream group.")
data class GroupKey(
    @field:Schema(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
    val streamPrefix: String,
    @field:Schema(description = "Redis Stream consumer group name.", example = "demo-workers")
    val consumerGroup: String,
)

@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Zero-based shard identifier inside one sharded Redis Stream group.")
data class ShardId(
    @field:Schema(description = "Zero-based shard index.", example = "4")
    val shardIndex: Int,
) : Comparable<ShardId> {
    override fun compareTo(other: ShardId): Int =
        compareValuesBy(this, other, ShardId::shardIndex)
}

@Schema(description = "Request body for creating coordinator-owned group metadata.")
data class CreateGroupRequest(
    @field:Min(1)
    @field:Schema(description = "Initial physical shard stream count. Omitted value uses coordinator defaults.", example = "20")
    val initialShardCount: Int? = null,
    @field:NotBlank
    @field:Schema(description = "Operator or automation identity requesting the change.", example = "platform-admin")
    val requestedBy: String,
    @field:Schema(description = "Human-readable reason stored in audit logs.", example = "create production order stream")
    val reason: String? = null,
)

@Schema(description = "Request body for creating coordinator-owned stream metadata.")
data class CreateStreamRequest(
    @field:Min(1)
    @field:Schema(description = "Initial physical shard stream count. Omitted value uses coordinator defaults.", example = "20")
    val initialShardCount: Int? = null,
    @field:NotBlank
    @field:Schema(description = "Operator or automation identity requesting the change.", example = "platform-admin")
    val requestedBy: String,
    @field:Schema(description = "Human-readable reason stored in audit logs.", example = "create production order stream")
    val reason: String? = null,
) {
    fun toGroupRequest(): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = requestedBy,
            reason = reason,
        )
}

@Schema(description = "Response for creating a stream-level shard layout.")
data class StreamCreateResponse(
    @field:Schema(description = "Sharded Redis Stream prefix whose shard layout was created.", example = "create-order")
    val streamPrefix: String,
    @field:Schema(description = "Initial physical shard count.", example = "20")
    val shardCount: Int,
    @field:Schema(description = "Coordinator metadata version created for the stream.", example = "1")
    val metadataVersion: Long,
)

@Schema(description = "Request body for deleting group metadata.")
data class DeleteGroupRequest(
    @field:NotBlank
    @field:Schema(description = "Operator or automation identity requesting the delete.", example = "platform-admin")
    val requestedBy: String,
    @field:NotBlank
    @field:Schema(description = "Reason for deleting group metadata.", example = "retire inactive test group")
    val reason: String,
    @field:Schema(description = "When true, delete even if live members are still registered.", example = "false")
    val force: Boolean = false,
)

@Schema(description = "Request body for changing physical shard count.")
data class ScaleGroupRequest(
    @field:Min(1)
    @field:Schema(description = "Target physical shard stream count.", example = "40")
    val targetShardCount: Int,
    @field:NotBlank
    @field:Schema(description = "Operator or automation identity requesting resharding.", example = "platform-admin")
    val requestedBy: String,
    @field:NotBlank
    @field:Schema(description = "Reason for the shard-count change.", example = "increase write throughput")
    val reason: String,
    @field:Schema(description = "Optional operator note for when old shards may be retired.")
    val deprecatedAfter: String? = null,
)

@Schema(description = "Request body for changing a stream's physical shard count across all registered consumer groups.")
data class ScaleStreamRequest(
    @field:Min(1)
    @field:Schema(description = "Target physical shard stream count.", example = "40")
    val targetShardCount: Int,
    @field:NotBlank
    @field:Schema(description = "Operator or automation identity requesting resharding.", example = "platform-admin")
    val requestedBy: String,
    @field:NotBlank
    @field:Schema(description = "Reason for the shard-count change.", example = "increase write throughput")
    val reason: String,
    @field:Schema(description = "Optional operator note for when old shards may be retired.")
    val deprecatedAfter: String? = null,
) {
    fun toGroupRequest(): ScaleGroupRequest =
        ScaleGroupRequest(
            targetShardCount = targetShardCount,
            requestedBy = requestedBy,
            reason = reason,
            deprecatedAfter = deprecatedAfter,
        )
}

@Schema(description = "Response for a stream-scoped shard-count change across all consumer groups of the stream.")
data class StreamScaleResponse(
    @field:Schema(description = "Sharded Redis Stream prefix whose shard count was changed.", example = "create-order")
    val streamPrefix: String,
    @field:Schema(description = "Requested physical shard count.", example = "40")
    val targetShardCount: Int,
    @field:Schema(description = "Consumer groups affected by this stream-level shard-count change.")
    val affectedConsumerGroups: List<String>,
    @field:Schema(description = "Per-consumer-group resharding results. Consumer groups converge on heartbeat.")
    val migrations: List<Migration>,
)

@Schema(description = "Request body for rolling back an active resharding operation.")
data class RollbackMigrationRequest(
    @field:NotBlank
    @field:Schema(description = "Operator or automation identity requesting rollback.", example = "platform-admin")
    val requestedBy: String,
    @field:NotBlank
    @field:Schema(description = "Reason for rollback.", example = "consumer drain did not converge")
    val reason: String,
)

@Schema(description = "Runtime capacity reported by a consumer member on heartbeat.")
data class RuntimeConsumerCapacity(
    @field:Min(1)
    @field:Schema(description = "Configured local worker capacity for this runtime member.", example = "1")
    val runtimeMaxConcurrency: Int,
    @field:Min(0)
    @field:Schema(description = "Currently available worker slots.", example = "1")
    val availableConcurrency: Int,
)

@Schema(description = "Consumer report for one shard being revoked from local ownership.")
data class RevokingShardReport(
    @field:Schema(description = "Shard being revoked.")
    val shard: ShardId,
    @field:Schema(description = "Current local revoke/drain state.")
    val state: RevokingShardState,
    @field:Schema(description = "In-flight handler count for this shard.", example = "0")
    val inFlight: Int = 0,
    @field:Schema(description = "Time when local drain completed.")
    val ackedAt: Instant? = null,
)

@Schema(description = "Latest per-shard consumption progress reported by a consumer member.")
data class ShardConsumptionProgress(
    @field:Schema(description = "Shard this progress row belongs to.")
    val shard: ShardId,
    @field:Schema(description = "Physical Redis Stream key.", example = "create-order:4")
    val streamKey: String,
    @field:Schema(description = "Last Redis Stream id delivered to the member.", example = "1780314922366-0")
    val lastDeliveredId: String? = null,
    @field:Schema(description = "Last Redis Stream id explicitly acknowledged by the member.", example = "1780314922366-0")
    val lastAckedId: String? = null,
    @field:Schema(description = "Local pending/in-flight count reported by the member.", example = "0")
    val pendingCount: Long = 0,
    @field:Schema(description = "Time when this progress row was updated.")
    val updatedAt: Instant? = null,
)

@Schema(description = "Consumer heartbeat request used for join, lease renewal, leave, revoke progress, and shard progress reporting.")
data class HeartbeatRequest(
    @field:Min(1)
    @field:Schema(description = "Coordination protocol version supported by the consumer module.", example = "1")
    val protocolVersion: Int,
    @field:NotBlank
    @field:Schema(description = "Idempotency/correlation id for this heartbeat attempt.", example = "hb-member-a-000042")
    val requestId: String,
    @field:NotBlank
    @field:Schema(description = "Runtime member id. Must match the path memberId.", example = "consumer-pod-0-m0")
    val memberId: String,
    @field:Schema(description = "Stable member name used for policy overrides. Usually the consumer group name.")
    val memberName: String? = null,
    @field:Schema(description = "Coordinator-issued member epoch. 0 joins/rejoins, -1 leaves, positive values must match the last response.", example = "11")
    val memberEpoch: Long,
    @field:Schema(description = "Metadata version currently cached by the consumer.", example = "7")
    val metadataVersion: Long,
    @field:Valid
    @field:Schema(description = "Runtime worker capacity and current availability.")
    val runtimeConsumerCapacity: RuntimeConsumerCapacity,
    @field:Schema(description = "Shards the member currently owns and may read.")
    val ownedShards: Set<ShardId> = emptySet(),
    @field:Schema(description = "Shards the member is draining after coordinator revocation.")
    val revokingShards: List<RevokingShardReport> = emptyList(),
    @field:Schema(description = "Latest per-shard consumption progress.")
    val shardProgress: List<ShardConsumptionProgress> = emptyList(),
)

@Schema(description = "Assignment view a member should converge to.")
data class AssignmentView(
    @field:Schema(description = "Shards this member may read now.")
    val assignedShards: Set<ShardId>,
    @field:Schema(description = "Target shards blocked until a previous owner finishes revoke-before-assign.")
    val pendingShards: Set<ShardId>,
    @field:Schema(description = "Metadata version this assignment was computed from.", example = "7")
    val metadataVersion: Long,
)

@Schema(description = "Coordinator response to one consumer heartbeat.")
data class HeartbeatResponse(
    @field:Schema(description = "Heartbeat request id this response corresponds to.", example = "hb-member-a-000042")
    val responseTo: String,
    @field:Schema(description = "Heartbeat handling status.")
    val status: HeartbeatStatus,
    @field:Schema(description = "Member id echoed by the coordinator.", example = "consumer-pod-0-m0")
    val memberId: String,
    @field:Schema(description = "Member epoch the consumer must use on the next heartbeat.", example = "12")
    val memberEpoch: Long,
    @field:Schema(description = "Recommended next heartbeat interval in milliseconds.", example = "5000")
    val heartbeatIntervalMs: Long,
    @field:Schema(description = "Maximum revoke/drain wait before the coordinator may fence a stuck member.", example = "10000")
    val rebalanceTimeoutMs: Long = CoordinatorProtocol.DEFAULT_TIMING.rebalanceTimeout.toMillis(),
    @field:Schema(description = "Group epoch for membership or assignment-affecting changes.", example = "12")
    val groupEpoch: Long,
    @field:Schema(description = "Assignment epoch for the target assignment currently in force.", example = "12")
    val assignmentEpoch: Long,
    @field:Schema(description = "Current coordinator metadata version.", example = "7")
    val metadataVersion: Long,
    @field:Schema(description = "Assigned and pending shard view for this member.")
    val assignment: AssignmentView,
)

@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Metadata for one coordinator-managed resharding operation.")
data class Migration(
    @param:JsonAlias("migrationId")
    @field:JsonAlias("migrationId")
    val reshardingId: String,
    @field:Schema(description = "Shard count before the operation.", example = "20")
    val fromShardCount: Int,
    @field:Schema(description = "Target shard count for the operation.", example = "40")
    val toShardCount: Int,
    @field:Schema(description = "Current resharding state.")
    var state: MigrationState,
    @field:Schema(description = "Creation time.")
    val createdAt: Instant,
    @field:Schema(description = "Last update time.")
    var updatedAt: Instant,
)

@Schema(description = "Coordinator metadata for one consumer member.")
data class MemberMetadata(
    @field:Schema(description = "Runtime member id.", example = "consumer-pod-0-m0")
    val memberId: String,
    @field:Schema(description = "Stable member name used for server-side policy overrides.")
    var memberName: String,
    @field:Schema(description = "Coordinator member lifecycle state.")
    var state: MemberState,
    @field:Schema(description = "Coordinator-issued member epoch.", example = "12")
    var memberEpoch: Long,
    @field:Schema(description = "Metadata version observed by this member.", example = "7")
    var metadataVersion: Long,
    @field:Schema(description = "Runtime worker capacity reported by the member.", example = "1")
    var runtimeMaxConcurrency: Int,
    @field:Schema(description = "Currently busy local workers reported by the member.", example = "0")
    var activeConsumerWorkers: Int,
    @field:Schema(description = "Shards currently owned by this member.")
    var currentAssignment: Set<ShardId>,
    @field:Schema(description = "Last assignable shard set granted to this member by a heartbeat response.")
    var grantedAssignment: Set<ShardId> = emptySet(),
    @field:Schema(description = "Shards currently being revoked by this member.")
    var revoking: Set<ShardId>,
    @field:Schema(description = "Last heartbeat time.")
    var lastHeartbeatAt: Instant,
    @field:Schema(description = "Lease deadline after which the member is considered expired.")
    var memberLeaseExpiresAt: Instant,
    @field:Schema(description = "Rebalance timeout currently applied to this member in milliseconds.", example = "10000")
    var rebalanceTimeoutMs: Long = 60_000,
    @field:Schema(description = "Deadline for revoke/drain before fencing.")
    var rebalanceDeadlineAt: Instant? = null,
    @field:Schema(description = "Latest per-shard progress reported by this member.")
    var shardProgress: List<ShardConsumptionProgress> = emptyList(),
)

@Schema(description = "In-progress metadata correction round for consumers that report a future metadata version.")
data class MetadataCorrection(
    val targetMetadataVersion: Long,
    var observedMetadataVersion: Long,
    val requestedAt: Instant,
    var updatedAt: Instant,
    val acknowledgedMembers: MutableSet<String> = linkedSetOf(),
)

@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Source-of-truth metadata stored by the coordinator for one sharded Redis Stream group.")
data class GroupMetadata(
    val streamPrefix: String,
    val consumerGroup: String,
    var schemaVersion: Int = COORDINATOR_METADATA_SCHEMA_VERSION,
    var storeRevision: Long = 0,
    var groupEpoch: Long,
    var metadataVersion: Long,
    var assignmentEpoch: Long,
    var state: GroupState,
    var shardCount: Int,
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

@Schema(description = "Public group metadata response returned by admin and monitoring APIs.")
data class GroupResponse(
    @field:Schema(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
    val streamPrefix: String,
    @field:Schema(description = "Redis Stream consumer group name.", example = "demo-workers")
    val consumerGroup: String,
    @field:Schema(description = "Current group lifecycle state.")
    val state: GroupState,
    @field:Schema(description = "Epoch for membership or assignment-affecting changes.", example = "12")
    val groupEpoch: Long,
    @field:Schema(description = "Epoch for the current target assignment.", example = "12")
    val assignmentEpoch: Long,
    @field:Schema(description = "Coordinator metadata version.", example = "7")
    val metadataVersion: Long,
    @field:Schema(description = "Current physical shard count.", example = "20")
    val shardCount: Int,
    @field:Schema(description = "Active resharding operation, if any.")
    val activeMigration: Migration?,
    @field:Schema(description = "Target shard count per member id.")
    val targetAssignmentSummary: Map<String, Int>,
    @field:Schema(description = "Current owned shard count per member id.")
    val currentAssignmentSummary: Map<String, Int>,
)

@Schema(description = "One physical shard route used by producers.")
data class ProducerRoutingShard(
    @field:Schema(description = "Zero-based shard index.", example = "4")
    val shardIndex: Int,
    @field:Schema(description = "Physical Redis Stream key.", example = "create-order:4")
    val streamKey: String,
    @field:Schema(description = "Redis Cluster hash slot for the stream key.", example = "12345")
    val redisSlot: Int,
)

@Schema(description = "Producer routing metadata for mapping partition keys to physical shard streams.")
data class ProducerRoutingResponse(
    @field:Schema(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
    val streamPrefix: String,
    @field:Schema(description = "Redis Stream consumer group name.", example = "demo-workers")
    val consumerGroup: String,
    @field:Schema(description = "Metadata version used for this routing view.", example = "7")
    val metadataVersion: Long,
    @field:Schema(description = "Physical shard count.", example = "20")
    val shardCount: Int,
    @field:Schema(description = "Pattern used to format physical stream keys.", example = "create-order:{shardIndex}")
    val streamKeyPattern: String,
    @field:Schema(description = "Concrete shard routes.")
    val shards: List<ProducerRoutingShard>,
)

@Schema(description = "Coordinator health response.")
data class HealthResponse(
    @field:Schema(description = "Overall coordinator status.", example = "UP")
    val status: String,
    @field:Schema(description = "Coordinator instance id.", example = "local-coordinator")
    val coordinatorId: String,
    @field:Schema(description = "Redis dependency status.", example = "UP")
    val redis: String,
    @field:Schema(description = "Coordinator loop status.", example = "UP")
    val loop: String,
)
@Schema(description = "Authenticated monitoring session response.")
data class MonitoringSessionResponse(
    @field:Schema(description = "Whether the current request is authenticated.", example = "true")
    val authenticated: Boolean,
    @field:Schema(description = "Authenticated username when available.", example = "admin")
    val username: String?,
    @field:Schema(description = "Configured API roles for this principal.", example = "[\"READ\", \"WRITE\"]")
    val roles: List<String> = emptyList(),
)

@Schema(description = "Login request for issuing a coordinator API token.")
data class LoginRequest(
    @field:Schema(description = "Coordinator API username.", example = "admin")
    val username: String = "",
    @field:Schema(description = "Coordinator API password.", example = "password")
    val password: String = "",
)

@Schema(description = "Bearer token response for coordinator API calls.")
data class LoginResponse(
    @field:Schema(description = "Signed bearer token. Send this as Authorization: Bearer <token>.")
    val accessToken: String,
    @field:Schema(description = "Token type.", example = "Bearer")
    val tokenType: String,
    @field:Schema(description = "Token expiration timestamp.")
    val expiresAt: Instant,
    @field:Schema(description = "Seconds until token expiration.", example = "604800")
    val expiresInSeconds: Long,
    @field:Schema(description = "Roles granted to this token.", example = "[\"READ\", \"WRITE\", \"ADMIN\"]")
    val roles: List<String>,
)

@Schema(description = "List of coordinator groups.")
data class GroupsResponse(val groups: List<GroupResponse>)
@Schema(description = "List of member metadata rows for a group.")
data class MembersResponse(val members: List<MemberMetadata>)
@Schema(description = "Target/current assignment and invariant snapshot for one group.")
data class AssignmentsResponse(
    @field:Schema(description = "Target shard assignment by member id.")
    val targetAssignment: Map<String, Set<ShardId>>,
    @field:Schema(description = "Current reported shard ownership by member id.")
    val currentAssignments: Map<String, Set<ShardId>>,
    @field:Schema(description = "Revoking shard set by member id.")
    val revokeProgress: Map<String, Set<ShardId>>,
    @field:Schema(description = "Detected assignment invariants such as duplicate owners or missing owners.")
    val invariantViolations: List<String>,
)
@Schema(description = "Resharding migration list for one group.")
data class MigrationsResponse(
    val migrations: List<Migration>,
    val activeReshardingId: String?,
)

@Schema(description = "Flattened per-member per-shard consumption progress.")
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
@Schema(description = "Latest consumption progress rows reported by consumers.")
data class ConsumptionProgressResponse(val progress: List<ConsumerShardProgress>)

@Schema(description = "Monitoring row for one physical Redis Stream shard.")
data class StreamShardOffset(
    @field:Schema(description = "Sharded Redis Stream prefix used to build physical stream keys such as create-order:4.", example = "create-order")
    val streamPrefix: String,
    @field:Schema(description = "Redis Stream consumer group name.", example = "demo-workers")
    val consumerGroup: String,
    @field:Schema(description = "Shard identifier.")
    val shard: ShardId,
    @field:Schema(description = "Physical Redis Stream key.", example = "create-order:4")
    val streamKey: String,
    @field:Schema(description = "Redis Cluster slot for this stream key.")
    val redisSlot: Int,
    @field:Schema(description = "Redis node endpoint that owns this slot when cluster metadata is available.")
    val redisNodeEndpoint: String?,
    @field:Schema(description = "Redis cluster node id when available.")
    val redisNodeId: String?,
    val redisSlotRangeStart: Int?,
    val redisSlotRangeEnd: Int?,
    @field:Schema(description = "Redis XLEN for this shard.", example = "1234")
    val streamLength: Long,
    val firstRecordId: String?,
    val lastRecordId: String?,
    val lastGeneratedId: String?,
    val groupLastDeliveredId: String?,
    val consumerLastDeliveredId: String?,
    val consumerLastAckedId: String?,
    @field:Schema(description = "Redis consumer-group pending count.", example = "0")
    val pendingCount: Long,
    @field:Schema(description = "Redis XINFO GROUPS lag. Null means Redis cannot compute lag for this group yet.")
    val lag: Long?,
    @field:Schema(description = "Redis MEMORY USAGE for the stream key in bytes, when available.")
    val memoryUsageBytes: Long?,
    @field:Schema(description = "Live member ids currently owning this shard.")
    val ownerMemberIds: List<String>,
)
@Schema(description = "Aggregated shard offset, lag, pending, owner, node, and memory response.")
data class StreamShardOffsetsResponse(
    val streamPrefix: String,
    val consumerGroup: String,
    val shards: List<StreamShardOffset>,
    val totalStreamLength: Long,
    val totalPendingCount: Long,
    val totalLag: Long?,
    val totalMemoryUsageBytes: Long,
    val memoryUsageKnown: Boolean,
)

@Schema(description = "Message pagination direction for Redis Stream inspection.")
enum class StreamMessagePageDirection { FORWARD, BACKWARD }

@Schema(description = "One Redis Stream record returned by the message inspection API.")
data class StreamMessageRecord(
    val streamPrefix: String,
    val consumerGroup: String,
    val shard: ShardId,
    val streamKey: String,
    val recordId: String,
    val fields: Map<String, String>,
    val payload: String?,
    @field:Schema(description = "Epoch milliseconds decoded from the Redis Stream record id.")
    val recordTimestampMs: Long? = redisStreamRecordTimestampMs(recordId),
    @field:Schema(description = "Record timestamp decoded from the Redis Stream record id.")
    val recordTime: Instant? = recordTimestampMs?.let(Instant::ofEpochMilli),
)
@Schema(description = "Cursor-based Redis Stream message page for one shard.")
data class StreamMessagesPageResponse(
    val streamPrefix: String,
    val consumerGroup: String,
    val shard: ShardId,
    val streamKey: String,
    val direction: StreamMessagePageDirection,
    val limit: Int,
    val records: List<StreamMessageRecord>,
    val nextCursor: String?,
)

@Schema(description = "Flat group row optimized for Grafana REST panels.")
data class GrafanaGroupRow(
    val streamPrefix: String,
    val consumerGroup: String,
    val state: GroupState,
    val groupEpoch: Long,
    val assignmentEpoch: Long,
    val metadataVersion: Long,
    val shardCount: Int,
    val activeMembers: Int,
    val totalMembers: Int,
    val currentShards: Int,
    val targetShards: Int,
    @field:Schema(description = "Operator-friendly current assignment ratio, formatted as currentShards/shardCount.", example = "20 / 20")
    val assignedShardRatio: String,
    val revokingShards: Int,
    val totalStreamLength: Long,
    val totalPendingCount: Long,
    val totalLag: Long?,
    @field:Schema(description = "Observed Redis Stream entry growth rate across shards. Null until at least two observations are available.", example = "1000.5")
    val producedPerSecond: Double?,
    @field:Schema(description = "Estimated consumer catch-up rate across shards, calculated from stream length and lag deltas. Null when lag is unknown or only one observation exists.", example = "950.25")
    val consumedPerSecond: Double?,
    val totalMemoryUsageBytes: Long,
    val memoryUsageKnown: Boolean,
)

@Schema(description = "Flat member row optimized for Grafana REST panels.")
data class GrafanaMemberRow(
    val streamPrefix: String,
    val consumerGroup: String,
    val memberId: String,
    val memberName: String,
    val state: MemberState,
    val memberEpoch: Long,
    val metadataVersion: Long,
    val currentShardCount: Int,
    val revokingShardCount: Int,
    val runtimeMaxConcurrency: Int,
    val activeConsumerWorkers: Int,
    val lastHeartbeatAt: Instant,
    val memberLeaseExpiresAt: Instant,
)

@Schema(description = "Flat shard row optimized for Grafana REST panels.")
data class GrafanaShardRow(
    val streamPrefix: String,
    val consumerGroup: String,
    val shardCount: Int,
    val shardIndex: Int,
    val shardLabel: String,
    val streamKey: String,
    val redisSlot: Int,
    val redisNodeEndpoint: String?,
    val redisNodeId: String?,
    val redisSlotRangeStart: Int?,
    val redisSlotRangeEnd: Int?,
    val streamLength: Long,
    val firstRecordId: String?,
    val lastRecordId: String?,
    val lastGeneratedId: String?,
    val groupLastDeliveredId: String?,
    val consumerLastDeliveredId: String?,
    val consumerLastAckedId: String?,
    val pendingCount: Long,
    val lag: Long?,
    val lagKnown: Boolean,
    @field:Schema(description = "Observed Redis Stream entry growth rate for this shard. Null until at least two observations are available.", example = "100.0")
    val producedPerSecond: Double?,
    @field:Schema(description = "Estimated consumer catch-up rate for this shard, calculated from stream length and lag deltas. Null when lag is unknown or only one observation exists.", example = "95.0")
    val consumedPerSecond: Double?,
    val memoryUsageBytes: Long?,
    val memoryUsageKnown: Boolean,
    val targetOwnerMemberIds: String,
    val currentOwnerMemberIds: String,
    val ownerState: String,
    val ownerMemberIds: String,
)

@Schema(description = "Flat target/current/revoking assignment row optimized for Grafana REST panels.")
data class GrafanaAssignmentRow(
    val streamPrefix: String,
    val consumerGroup: String,
    val shardIndex: Int,
    val targetOwners: String,
    val currentOwners: String,
    val revokingOwners: String,
)

@Schema(description = "Grafana variable option row.")
data class GrafanaOptionRow(
    @get:JsonProperty("__text")
    val text: String,
    @get:JsonProperty("__value")
    val value: String,
)

@Schema(description = "Flat Redis Stream message row optimized for Grafana table panels.")
data class GrafanaMessageRow(
    val streamPrefix: String,
    val consumerGroup: String,
    val shardIndex: Int,
    val shardSelector: String,
    val shardLabel: String,
    val streamKey: String,
    val recordId: String,
    val payload: String?,
    val fieldsJson: String,
    val pageDirection: StreamMessagePageDirection,
    val pageLimit: Int,
    val pageNextCursor: String?,
    val pageTotalMessages: Long,
    @field:Schema(description = "Epoch milliseconds decoded from the Redis Stream record id.")
    val recordTimestampMs: Long? = redisStreamRecordTimestampMs(recordId),
    @field:Schema(description = "Record timestamp decoded from the Redis Stream record id.")
    val recordTime: Instant? = recordTimestampMs?.let(Instant::ofEpochMilli),
)

fun redisStreamRecordTimestampMs(recordId: String): Long? =
    recordId.substringBefore('-', missingDelimiterValue = "")
        .takeIf { it.isNotBlank() }
        ?.toLongOrNull()

fun newReshardingId(): String = "reshard-${UUID.randomUUID()}"
