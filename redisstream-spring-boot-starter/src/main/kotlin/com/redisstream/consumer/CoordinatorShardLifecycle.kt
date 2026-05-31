package com.redisstream.consumer

interface CoordinatorShardLifecycle {
    /**
     * Starts or resumes local work for shards that the coordinator has assigned to this member.
     */
    fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext)

    /**
     * Stops new work for revoked shards and returns the subset that has fully drained.
     */
    fun onRevoked(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext): Set<CoordinatorShard> = shards

    /**
     * Observes target shards that are blocked until the previous owner reports revoke completion.
     */
    fun onPending(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
    }

    /**
     * Clears local ownership when the coordinator rejects this member epoch or identity.
     */
    fun onFenced(context: CoordinatorConsumerContext) {
    }
}

interface CoordinatorRuntimeCapacityProvider {
    /**
     * Reports runtime capacity so the coordinator can assign work according to current local load.
     */
    fun runtimeCapacity(context: CoordinatorConsumerContext): RuntimeConsumerCapacity
}

interface CoordinatorShardProgressProvider {
    /**
     * Reports per-shard Redis Stream progress so the coordinator can expose centralized metrics.
     */
    fun shardProgress(context: CoordinatorConsumerContext): List<ShardConsumptionProgress>
}
