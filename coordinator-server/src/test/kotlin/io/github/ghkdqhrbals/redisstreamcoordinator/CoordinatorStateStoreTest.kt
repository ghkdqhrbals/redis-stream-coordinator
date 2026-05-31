package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class CoordinatorStateStoreTest {
    private val clock = Clock.fixed(Instant.parse("2026-05-21T00:00:00Z"), ZoneOffset.UTC)
    private val properties = CoordinatorProperties(
        heartbeatInterval = Duration.ofSeconds(3),
        memberLeaseTtl = Duration.ofSeconds(15),
        defaults = CoordinatorProperties.Defaults(
            initialShardCount = 4,
            consumerMaxConcurrency = 4,
        ),
    )

    @Test
    fun `memory store creates saves and lists groups`() {
        val store = InMemoryCoordinatorStateStore()
        val key = GroupKey("orders", "orders-consumer")
        val group = groupMetadata(key)

        assertFalse(store.contains(key))
        assertTrue(store.putIfAbsent(key, group))
        assertFalse(store.putIfAbsent(key, group.copy(metadataVersion = 99)))

        val stored = assertNotNull(store.get(key))
        assertEquals(1, stored.metadataVersion)

        store.save(key, stored.copy(metadataVersion = 2))

        assertTrue(store.contains(key))
        assertEquals(2, store.get(key)?.metadataVersion)
        assertEquals(listOf(key), store.list().map { GroupKey(it.streamPrefix, it.consumerGroup) })

        val currentRevision = assertNotNull(store.get(key)).storeRevision
        assertFalse(store.deleteIfRevision(key, currentRevision - 1))
        assertTrue(store.contains(key))
        assertTrue(store.deleteIfRevision(key, currentRevision))
        assertFalse(store.contains(key))
    }

    @Test
    fun `memory store survives coordinator replacement`() {
        val store = InMemoryCoordinatorStateStore()
        val firstService = service(store)
        firstService.createGroup("payments", "payments-consumer", createGroupRequest(initialShardCount = 2))
        val heartbeat = firstService.heartbeat(
            "payments",
            "payments-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )

        val replacementService = service(store)
        val group = replacementService.getGroup("payments", "payments-consumer")
        val members = replacementService.listMembers("payments", "payments-consumer")

        assertEquals(1, group.targetAssignmentSummary.size)
        assertEquals(2, group.targetAssignmentSummary.getValue("member-a"))
        assertEquals(heartbeat.memberEpoch, members.members.single().memberEpoch)
    }

    @Test
    fun `redis keys keep group metadata in one hash slot`() {
        val stateKeys = RedisCoordinatorStateKeys("redis-stream:coord:")
        val keys = stateKeys.forGroup(GroupKey("orders", "orders-consumer"))

        assertEquals("redis-stream:coord::groups", stateKeys.groupsIndex)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:metadata", keys.metadata)
    }

    private fun service(store: CoordinatorStateStore): CoordinatorService =
        CoordinatorService(
            properties = properties,
            stateStore = store,
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = NoopStreamShardProvisioner,
            clock = clock,
        )

    private fun groupMetadata(key: GroupKey): GroupMetadata =
        GroupMetadata(
            streamPrefix = key.streamPrefix,
            consumerGroup = key.consumerGroup,
            groupEpoch = 1,
            metadataVersion = 1,
            assignmentEpoch = 0,
            state = GroupState.EMPTY,
            activeWriteVersion = 1,
            readableVersions = setOf(1),
            shardCountsByVersion = linkedMapOf(1 to 4),
            consumerConcurrencyPolicy = ConsumerConcurrencyPolicy(defaultMaxConcurrency = 4),
            createdAt = Instant.now(clock),
            updatedAt = Instant.now(clock),
        )

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun heartbeat(memberId: String, memberEpoch: Long): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = 1,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            rebalanceTimeoutMs = 60_000,
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
        )
}
