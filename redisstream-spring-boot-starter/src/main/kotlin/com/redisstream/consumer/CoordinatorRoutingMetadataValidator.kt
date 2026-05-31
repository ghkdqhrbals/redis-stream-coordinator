package com.redisstream.consumer

internal object CoordinatorRoutingMetadataValidator {
    fun validate(
        streamPrefix: String,
        consumerGroupName: String,
        metadata: ProducerRoutingResponse,
    ) {
        require(metadata.streamPrefix == streamPrefix) {
            "producer routing streamPrefix ${metadata.streamPrefix} does not match configured $streamPrefix"
        }
        require(metadata.consumerGroup == consumerGroupName) {
            "producer routing consumerGroup ${metadata.consumerGroup} does not match configured $consumerGroupName"
        }
        require(metadata.shardCount > 0) {
            "coordinator group $streamPrefix/$consumerGroupName has no active shards"
        }
        require(metadata.shards.isNotEmpty()) {
            "coordinator group $streamPrefix/$consumerGroupName has no shard metadata"
        }
        val activeShardIndexes = metadata.shards
            .filter { it.streamVersion == metadata.activeWriteVersion }
            .map { it.shardIndex }
            .toSortedSet()
        require(activeShardIndexes == (0 until metadata.shardCount).toSortedSet()) {
            "coordinator group $streamPrefix/$consumerGroupName active shard list does not match shardCount"
        }
    }
}
