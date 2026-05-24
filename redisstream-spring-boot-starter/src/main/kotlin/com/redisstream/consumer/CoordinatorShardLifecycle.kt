package com.redisstream.consumer

interface CoordinatorShardLifecycle {
    fun onAssigned(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext)

    fun onRevoked(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext): Set<CoordinatorShard> = shards

    fun onPending(shards: Set<CoordinatorShard>, context: CoordinatorConsumerContext) {
    }

    fun onFenced(context: CoordinatorConsumerContext) {
    }
}

interface CoordinatorRuntimeCapacityProvider {
    fun runtimeCapacity(context: CoordinatorConsumerContext): RuntimeConsumerCapacity
}
