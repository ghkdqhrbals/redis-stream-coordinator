package com.redisstream.consumer

import java.time.Instant

enum class HeartbeatStatus { OK, RETRY, UNKNOWN_MEMBER_ID, FENCED_MEMBER_EPOCH, UNSUPPORTED_PROTOCOL, INVALID_REQUEST }
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
    val hashAlgorithm: String,
    val hashSeed: String,
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
