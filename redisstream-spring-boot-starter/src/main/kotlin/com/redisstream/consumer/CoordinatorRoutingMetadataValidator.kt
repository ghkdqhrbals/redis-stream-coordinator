package com.redisstream.consumer

internal object CoordinatorRoutingMetadataValidator {
    fun validate(
        streamPrefix: String,
        metadata: ProducerRoutingResponse,
    ) {
        require(metadata.streamPrefix == streamPrefix) {
            "producer routing streamPrefix ${metadata.streamPrefix} does not match configured $streamPrefix"
        }
        require(metadata.shardCount > 0) {
            "coordinator stream $streamPrefix has no active shards"
        }
        require(metadata.shards.isNotEmpty()) {
            "coordinator stream $streamPrefix has no shard metadata"
        }
        val activeShardIndexes = metadata.shards.map { it.shardIndex }.toSortedSet()
        require(activeShardIndexes == (0 until metadata.shardCount).toSortedSet()) {
            "coordinator stream $streamPrefix active shard list does not match shardCount"
        }
    }
}
