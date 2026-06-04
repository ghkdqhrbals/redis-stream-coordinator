package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.CreateGroupRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatRequest
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.HeartbeatStatus
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.RuntimeConsumerCapacity
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardConsumptionProgress
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.InMemoryCoordinatorStateStore
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.NoopStreamShardProvisioner
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.time.Duration
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class CoordinatorConsumptionProgressTest {
    @Test
    fun `coordinator stores consumer reported shard progress`() {
        val service = service()
        service.createGroup("orders", "orders-consumer", CreateGroupRequest(initialShardCount = 2, requestedBy = "test"))
        val first = service.heartbeat("orders", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        val shard = first.assignment.assignedShards.first()
        val progress = ShardConsumptionProgress(
            shard = shard,
            streamKey = "orders:${shard.shardIndex}",
            lastDeliveredId = "100-2",
            lastAckedId = "100-1",
            pendingCount = 1,
            updatedAt = Instant.parse("2026-05-28T00:00:00Z"),
        )

        service.heartbeat(
            "orders",
            "orders-consumer",
            "member-a",
            heartbeat(
                memberId = "member-a",
                memberEpoch = first.memberEpoch,
                ownedShards = first.assignment.assignedShards,
                shardProgress = listOf(progress),
            ),
        )

        val response = service.consumptionProgress("orders", "orders-consumer")

        assertEquals(1, response.progress.size)
        assertEquals("member-a", response.progress.single().memberId)
        assertEquals(shard, response.progress.single().shard)
        assertEquals("100-2", response.progress.single().lastDeliveredId)
        assertEquals("100-1", response.progress.single().lastAckedId)
        assertEquals(1L, response.progress.single().pendingCount)
    }

    @Test
    fun `coordinator fences progress reports for unassigned shards`() {
        val service = service()
        service.createGroup("orders", "orders-consumer", CreateGroupRequest(initialShardCount = 1, requestedBy = "test"))
        val first = service.heartbeat("orders", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))

        val response = service.heartbeat(
            "orders",
            "orders-consumer",
            "member-a",
            heartbeat(
                memberId = "member-a",
                memberEpoch = first.memberEpoch,
                ownedShards = first.assignment.assignedShards,
                shardProgress = listOf(
                    ShardConsumptionProgress(
                        shard = ShardId(99),
                        streamKey = "orders:99",
                        lastDeliveredId = "100-0",
                        lastAckedId = "100-0",
                    ),
                ),
            ),
        )

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, response.status)
    }

    private fun service(): CoordinatorService =
        CoordinatorService(
            properties = CoordinatorProperties(
                heartbeatInterval = Duration.ofSeconds(3),
                memberLeaseTtl = Duration.ofSeconds(15),
                defaults = CoordinatorProperties.Defaults(initialShardCount = 2, consumerMaxConcurrency = 4),
            ),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = NoopStreamShardProvisioner,
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
        shardProgress: List<ShardConsumptionProgress> = emptyList(),
    ): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
            ownedShards = ownedShards,
            shardProgress = shardProgress,
        )
}
