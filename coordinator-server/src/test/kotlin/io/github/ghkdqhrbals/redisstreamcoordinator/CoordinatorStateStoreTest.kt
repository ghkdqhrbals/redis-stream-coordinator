package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.data.redis.core.script.RedisScript
import tools.jackson.databind.ObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
    fun `jdbc store persists group metadata and rejects stale writes`() {
        val store = jdbcStore()
        val key = GroupKey("jdbc-orders", "orders-consumer")
        val group = groupMetadata(key)

        assertTrue(store.putIfAbsent(key, group))
        assertFalse(store.putIfAbsent(key, groupMetadata(key).copy(metadataVersion = 99)))

        val firstSnapshot = assertNotNull(store.get(key))
        val staleSnapshot = assertNotNull(store.get(key))
        firstSnapshot.metadataVersion = 2
        store.save(key, firstSnapshot)

        staleSnapshot.metadataVersion = 3
        assertFailsWith<CoordinatorStateConflictException> {
            store.save(key, staleSnapshot)
        }

        val stored = assertNotNull(store.get(key))
        assertEquals(2, stored.metadataVersion)
        assertEquals(2, stored.storeRevision)
        assertEquals(listOf(key), store.list().map { GroupKey(it.streamPrefix, it.consumerGroup) })
        assertFalse(store.deleteIfRevision(key, expectedRevision = 1))
        assertTrue(store.deleteIfRevision(key, expectedRevision = 2))
        assertFalse(store.contains(key))
    }

    @Test
    fun `redis keys keep group metadata in one hash slot`() {
        val stateKeys = RedisCoordinatorStateKeys("redis-stream:coord:")
        val keys = stateKeys.forGroup(GroupKey("orders", "orders-consumer"))

        assertEquals("redis-stream:coord::groups", stateKeys.groupsIndex)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:group", keys.group)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:metadata", keys.metadata)
    }

    @Test
    fun `redis store reads and migrates legacy group value key`() {
        val objectMapper = ObjectMapper()
        val redis = FakeStateStoreRedisCommands()
        val store = RedisCoordinatorStateStore(
            redisCommands = redis,
            objectMapper = objectMapper,
            properties = properties,
        )
        val key = GroupKey("legacy-orders", "orders-consumer")
        val keys = RedisCoordinatorStateKeys(properties.store.keyPrefix).forGroup(key)
        val legacy = groupMetadata(key).also {
            it.metadataVersion = 7
            it.storeRevision = 4
        }
        redis.values[keys.group] = objectMapper.writeValueAsString(legacy)
        redis.setAdd(RedisCoordinatorStateKeys(properties.store.keyPrefix).groupsIndex, keys.group)

        assertTrue(store.contains(key))
        val loaded = assertNotNull(store.get(key))

        assertEquals(7, loaded.metadataVersion)
        assertEquals(4, loaded.storeRevision)
        assertEquals(7, store.list().single().metadataVersion)
        assertEquals(objectMapper.writeValueAsString(loaded), redis.hashes.getValue(keys.metadata).getValue("aggregate"))
        assertEquals("4", redis.hashes.getValue(keys.metadata).getValue("revision"))
    }

    @Test
    fun `redis store rejects put if legacy group value key already exists`() {
        val objectMapper = ObjectMapper()
        val redis = FakeStateStoreRedisCommands()
        val store = RedisCoordinatorStateStore(
            redisCommands = redis,
            objectMapper = objectMapper,
            properties = properties,
        )
        val key = GroupKey("legacy-duplicate", "orders-consumer")
        val keys = RedisCoordinatorStateKeys(properties.store.keyPrefix).forGroup(key)
        redis.values[keys.group] = objectMapper.writeValueAsString(groupMetadata(key))

        assertFalse(store.putIfAbsent(key, groupMetadata(key).copy(metadataVersion = 99)))

        val loaded = assertNotNull(store.get(key))
        assertEquals(1, loaded.metadataVersion)
        assertEquals("1", redis.hashes.getValue(keys.metadata).getValue("revision"))
    }

    private fun service(store: CoordinatorStateStore): CoordinatorService =
        CoordinatorService(
            properties = properties,
            stateStore = store,
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = NoopStreamShardProvisioner,
            clock = clock,
        )

    private fun jdbcStore(): JdbcCoordinatorStateStore {
        val dataSource = DriverManagerDataSource().apply {
            setDriverClassName("org.h2.Driver")
            url = "jdbc:h2:mem:${System.nanoTime()};MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
        }
        return JdbcCoordinatorStateStore(JdbcTemplate(dataSource), ObjectMapper())
    }

    private fun groupMetadata(key: GroupKey): GroupMetadata =
        GroupMetadata(
            streamPrefix = key.streamPrefix,
            consumerGroup = key.consumerGroup,
            groupEpoch = 1,
            metadataVersion = 1,
            assignmentEpoch = 0,
            state = GroupState.EMPTY,
            shardCount = 4,
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
            metadataVersion = 0,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
        )
}

private class FakeStateStoreRedisCommands : CoordinatorRedisCommands() {
    val values = mutableMapOf<String, String>()
    val hashes = mutableMapOf<String, MutableMap<String, String>>()
    private val sets = mutableMapOf<String, MutableSet<String>>()

    override fun hasKey(key: String): Boolean =
        key in values || key in hashes

    override fun getValue(key: String): String? =
        values[key]

    override fun hashGet(key: String, field: String): String? =
        hashes[key]?.get(field)

    override fun setAdd(key: String, value: String) {
        sets.getOrPut(key, ::linkedSetOf).add(value)
    }

    override fun setRemove(key: String, value: String) {
        sets[key]?.remove(value)
    }

    override fun setMembers(key: String): Set<String> =
        sets[key].orEmpty()

    override fun executeLong(script: RedisScript<Long>, keys: List<String>, vararg args: String): Long? {
        val key = keys.single()
        if (args.size == 1) {
            val revision = hashes[key]?.get("revision")
            if (revision != args[0]) {
                return 0
            }
            hashes.remove(key)
            return 1
        }

        val mode = args[0]
        val expectedRevision = args[1]
        val nextRevision = args[2]
        val aggregate = args[3]

        if (mode == "NX" && key in hashes) {
            return 0
        }
        if (mode != "NX") {
            val currentRevision = hashes[key]?.get("revision")
            if (currentRevision != null && currentRevision != expectedRevision) {
                return -1
            }
        }

        hashes[key] = mutableMapOf(
            "aggregate" to aggregate,
            "revision" to nextRevision,
            "schemaVersion" to args[4],
            "layoutVersion" to args[5],
            "updatedAt" to args[6],
        )
        return 1
    }
}
