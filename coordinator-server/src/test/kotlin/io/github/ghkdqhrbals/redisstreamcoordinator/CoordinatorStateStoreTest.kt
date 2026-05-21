package io.github.ghkdqhrbals.redisstreamcoordinator

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
    fun `in memory store supports create get save and list`() {
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
    }

    @Test
    fun `coordinator state survives service instance replacement when store is shared`() {
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
    fun `redis coordinator keys keep group scoped keys in the same cluster hash slot`() {
        val stateKeys = RedisCoordinatorStateKeys("redis-stream:coord:")
        val keys = stateKeys.forGroup(GroupKey("orders", "orders-consumer"))

        assertEquals("redis-stream:coord::groups", stateKeys.groupsIndex)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:group", keys.group)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:members", keys.members)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:target-assignments", keys.targetAssignments)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:current-assignments", keys.currentAssignments)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:migrations", keys.migrations)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:active-migration", keys.activeMigration)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:revision", keys.revision)
    }

    @Test
    fun `redis projection splits aggregate state into PRD key model sections`() {
        val key = GroupKey("orders", "orders-consumer")
        val group = groupMetadata(key)
        val member = MemberMetadata(
            memberId = "member-a",
            memberName = "member-a",
            state = MemberState.ACTIVE,
            memberEpoch = 2,
            metadataVersion = 3,
            assignedMaxConcurrency = 4,
            runtimeMaxConcurrency = 4,
            activeConsumerWorkers = 1,
            currentAssignment = setOf(ShardId(1, 0)),
            revoking = emptySet(),
            lastHeartbeatAt = Instant.now(clock),
            memberLeaseExpiresAt = Instant.now(clock).plusSeconds(15),
        )
        val migration = Migration(
            migrationId = "mig-1",
            fromVersion = 1,
            toVersion = 2,
            fromShardCount = 4,
            toShardCount = 8,
            state = MigrationState.ACTIVE,
            createdAt = Instant.now(clock),
            updatedAt = Instant.now(clock),
        )
        group.members[member.memberId] = member
        group.targetAssignments[member.memberId] = mutableSetOf(ShardId(1, 0), ShardId(1, 1))
        group.migrations[migration.migrationId] = migration
        group.activeMigrationId = migration.migrationId

        val projection = group.toRedisStateProjection()

        assertEquals(setOf("member-a"), projection.members.keys)
        assertEquals(setOf(ShardId(1, 0), ShardId(1, 1)), projection.targetAssignments.getValue("member-a"))
        assertEquals(setOf(ShardId(1, 0)), projection.currentAssignments.getValue("member-a"))
        assertEquals(setOf("mig-1"), projection.migrations.keys)
        assertEquals("mig-1", projection.activeMigrationId)
    }

    private fun service(store: CoordinatorStateStore): CoordinatorService =
        CoordinatorService(
            properties = properties,
            stateStore = store,
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
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
            hashAlgorithm = "murmur3",
            hashSeed = "default",
            consumerConcurrencyPolicy = ConsumerConcurrencyPolicy(defaultMaxConcurrency = 4),
            createdAt = Instant.now(clock),
            updatedAt = Instant.now(clock),
        )

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            hashAlgorithm = "murmur3",
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
