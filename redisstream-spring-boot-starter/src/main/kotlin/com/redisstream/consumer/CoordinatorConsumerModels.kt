package com.redisstream.consumer

import java.time.Instant

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

data class CoordinatorShard(
    val streamVersion: Int,
    val shardIndex: Int,
) : Comparable<CoordinatorShard> {
    override fun compareTo(other: CoordinatorShard): Int =
        compareValuesBy(this, other, CoordinatorShard::streamVersion, CoordinatorShard::shardIndex)
}

fun CoordinatorShard.streamKey(streamPrefix: String): String =
    "$streamPrefix:v$streamVersion:shard:$shardIndex"

data class RuntimeConsumerCapacity(
    val runtimeMaxConcurrency: Int,
    val availableConcurrency: Int,
)

data class RevokingShardReport(
    val shard: CoordinatorShard,
    val state: RevokingShardState,
    val inFlight: Int = 0,
    val ackedAt: Instant? = null,
)

data class ShardConsumptionProgress(
    val shard: CoordinatorShard,
    val streamKey: String,
    val lastDeliveredId: String? = null,
    val lastAckedId: String? = null,
    val pendingCount: Long = 0,
    val updatedAt: Instant? = null,
)

data class HeartbeatRequest(
    val protocolVersion: Int,
    val requestId: String,
    val memberId: String,
    val memberName: String? = null,
    val memberEpoch: Long,
    val rebalanceTimeoutMs: Long? = null,
    val metadataVersion: Long,
    val runtimeConsumerCapacity: RuntimeConsumerCapacity,
    val ownedShards: Set<CoordinatorShard> = emptySet(),
    val revokingShards: List<RevokingShardReport> = emptyList(),
    val shardProgress: List<ShardConsumptionProgress> = emptyList(),
)

data class AssignmentView(
    val assignedShards: Set<CoordinatorShard>,
    val pendingShards: Set<CoordinatorShard>,
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

data class CoordinatorConsumerContext(
    val memberId: String,
    val memberName: String,
    val assignedMaxConcurrency: Int,
    val metadataVersion: Long,
    val groupEpoch: Long,
    val assignmentEpoch: Long,
)
