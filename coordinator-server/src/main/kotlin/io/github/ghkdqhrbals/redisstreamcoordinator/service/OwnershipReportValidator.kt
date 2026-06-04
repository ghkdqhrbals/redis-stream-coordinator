package io.github.ghkdqhrbals.redisstreamcoordinator.service

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MemberMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MemberState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RevokingShardReport
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RevokingShardState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardConsumptionProgress
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId

internal data class ValidatedOwnershipReport(
    val ownedShards: Set<ShardId>,
    val revokingShards: List<RevokingShardReport>,
    val shardProgress: List<ShardConsumptionProgress>,
)

internal object OwnershipReportValidator {
    /**
     * A consumer report is observational only. Ownership is accepted only when it matches
     * previously accepted shards or currently assignable coordinator targets.
     */
    fun validate(
        group: GroupMetadata,
        member: MemberMetadata,
        request: HeartbeatRequest,
        readableShards: Set<ShardId>,
        blockedTargetShards: Set<ShardId>,
    ): ValidatedOwnershipReport? {
        if (request.memberEpoch == 0L) {
            return ValidatedOwnershipReport(emptySet(), emptyList(), emptyList())
        }

        val authority = OwnershipAuthority.from(
            group = group,
            member = member,
            readableShards = readableShards,
            blockedTargetShards = blockedTargetShards,
        )
        val report = ReportedOwnership.from(member, request)
        if (report.hasUnauthorizedShards(authority)) {
            return null
        }
        return report.acceptedBy(authority)
    }

    private data class OwnershipAuthority(
        val ownedShards: Set<ShardId>,
        val revokingShards: Set<ShardId>,
        val progressShards: Set<ShardId>,
    ) {
        companion object {
            fun from(
                group: GroupMetadata,
                member: MemberMetadata,
                readableShards: Set<ShardId>,
                blockedTargetShards: Set<ShardId>,
            ): OwnershipAuthority {
                val previouslyAccepted = member.currentAssignment + member.revoking + member.grantedAssignment
                val coordinatorGranted = group.targetAssignments[member.memberId].orEmpty()
                    .filter { it !in blockedTargetShards && it in readableShards }
                    .toSet()
                val ownedShards = previouslyAccepted + coordinatorGranted
                val revokingShards = previouslyAccepted + coordinatorGranted
                return OwnershipAuthority(
                    ownedShards = ownedShards,
                    revokingShards = revokingShards,
                    progressShards = ownedShards + revokingShards,
                )
            }
        }
    }

    private data class ReportedOwnership(
        val ownedShards: Set<ShardId>,
        val revokingShards: List<RevokingShardReport>,
        val progress: List<ShardConsumptionProgress>,
    ) {
        fun hasUnauthorizedShards(authority: OwnershipAuthority): Boolean =
            (ownedShards - authority.ownedShards).isNotEmpty() ||
                activeRevokingShards().any { it !in authority.revokingShards } ||
                progress.any { it.shard !in authority.progressShards }

        fun acceptedBy(authority: OwnershipAuthority): ValidatedOwnershipReport =
            ValidatedOwnershipReport(
                ownedShards = ownedShards.filter { it in authority.ownedShards }.toSet(),
                revokingShards = revokingShards.filter { it.shard in authority.revokingShards },
                shardProgress = progress
                    .filter { it.shard in authority.progressShards }
                    .sortedBy { it.shard },
            )

        private fun activeRevokingShards(): List<ShardId> =
            revokingShards
                .filterNot { it.state == RevokingShardState.REVOKED && it.inFlight == 0 }
                .map { it.shard }

        companion object {
            fun from(member: MemberMetadata, request: HeartbeatRequest): ReportedOwnership =
                ReportedOwnership(
                    ownedShards = if (member.state == MemberState.LEAVING) emptySet() else request.ownedShards,
                    revokingShards = request.revokingShards,
                    progress = request.shardProgress,
                )
        }
    }
}
