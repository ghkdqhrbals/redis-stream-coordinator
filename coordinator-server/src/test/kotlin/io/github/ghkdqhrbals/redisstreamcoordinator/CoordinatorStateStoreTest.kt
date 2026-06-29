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
import kotlin.test.assertNull
import kotlin.test.assertTrue
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.IdentityHashMap

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

        assertEquals("coordinator:metadata", stateKeys.coordinatorMetadata)
        assertEquals("redis-stream:coord::groups", stateKeys.legacyGroupsIndex)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:group", keys.group)
        assertEquals("redis-stream:coord::{orders:orders-consumer}:metadata", keys.metadata)
        assertEquals("group:redis-stream:coord::{orders:orders-consumer}:metadata", stateKeys.groupIndexMember(keys.metadata))
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
        val stateKeys = RedisCoordinatorStateKeys(properties.store.keyPrefix)
        redis.setAdd(stateKeys.legacyGroupsIndex, keys.group)

        assertTrue(store.contains(key))
        val loaded = assertNotNull(store.get(key))

        assertEquals(7, loaded.metadataVersion)
        assertEquals(4, loaded.storeRevision)
        assertEquals(7, store.list().single().metadataVersion)
        assertEquals(objectMapper.writeValueAsString(loaded), redis.hashes.getValue(keys.metadata).getValue("aggregate"))
        assertEquals("4", redis.hashes.getValue(keys.metadata).getValue("revision"))
        assertEquals(setOf(stateKeys.groupIndexMember(keys.metadata)), redis.setMembers(stateKeys.coordinatorMetadata))
        assertEquals(setOf(keys.group), redis.setMembers(stateKeys.legacyGroupsIndex))
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

    @Test
    fun `redis store writes coordinator index members to single metadata key`() {
        val objectMapper = ObjectMapper()
        val redis = FakeStateStoreRedisCommands()
        val store = RedisCoordinatorStateStore(
            redisCommands = redis,
            objectMapper = objectMapper,
            properties = properties,
        )
        val stateKeys = RedisCoordinatorStateKeys(properties.store.keyPrefix)
        val key = GroupKey("indexed-orders", "orders-consumer")
        val groupKeys = stateKeys.forGroup(key)
        val streamKey = stateKeys.forStream("indexed-orders")

        assertTrue(store.putIfAbsent(key, groupMetadata(key)))
        assertTrue(store.putStreamIfAbsent(streamMetadata("indexed-orders")))

        assertEquals(
            setOf(stateKeys.groupIndexMember(groupKeys.metadata), stateKeys.streamIndexMember(streamKey.metadata)),
            redis.setMembers(stateKeys.coordinatorMetadata),
        )
        assertEquals(emptySet(), redis.setMembers(stateKeys.legacyGroupsIndex))
        assertEquals(emptySet(), redis.setMembers(stateKeys.legacyStreamsIndex))
        assertEquals(listOf(key), store.list().map { GroupKey(it.streamPrefix, it.consumerGroup) })
        assertEquals(listOf("indexed-orders"), store.listStreams().map { it.streamPrefix })
    }

    @Test
    fun `redis store replaces empty group metadata hash during put if absent`() {
        val objectMapper = ObjectMapper()
        val redis = FakeStateStoreRedisCommands()
        val store = RedisCoordinatorStateStore(
            redisCommands = redis,
            objectMapper = objectMapper,
            properties = properties,
        )
        val key = GroupKey("corrupt-orders", "orders-consumer")
        val keys = RedisCoordinatorStateKeys(properties.store.keyPrefix).forGroup(key)
        redis.hashes[keys.metadata] = mutableMapOf(
            "aggregate" to "{}",
            "revision" to "1",
            "schemaVersion" to "1",
            "layoutVersion" to "1",
            "updatedAt" to Instant.now(clock).toString(),
        )

        assertTrue(store.putIfAbsent(key, groupMetadata(key)))

        val loaded = assertNotNull(store.get(key))
        assertEquals("corrupt-orders", loaded.streamPrefix)
        assertEquals("orders-consumer", loaded.consumerGroup)
        assertEquals(1, loaded.storeRevision)
        assertEquals(
            setOf(RedisCoordinatorStateKeys(properties.store.keyPrefix).groupIndexMember(keys.metadata)),
            redis.setMembers("coordinator:metadata"),
        )
    }

    @Test
    fun `redis store replaces empty stream metadata hash during put if absent`() {
        val objectMapper = ObjectMapper()
        val redis = FakeStateStoreRedisCommands()
        val store = RedisCoordinatorStateStore(
            redisCommands = redis,
            objectMapper = objectMapper,
            properties = properties,
        )
        val streamKey = RedisCoordinatorStateKeys(properties.store.keyPrefix).forStream("corrupt-stream")
        redis.hashes[streamKey.metadata] = mutableMapOf(
            "aggregate" to "{}",
            "revision" to "1",
            "schemaVersion" to "1",
            "layoutVersion" to "1",
            "updatedAt" to Instant.now(clock).toString(),
        )

        assertNull(store.getStream("corrupt-stream"))
        assertTrue(store.putStreamIfAbsent(streamMetadata("corrupt-stream")))

        val loaded = assertNotNull(store.getStream("corrupt-stream"))
        assertEquals("corrupt-stream", loaded.streamPrefix)
        assertEquals(1, loaded.storeRevision)
    }

    @Test
    fun `initial heartbeat recovers empty group metadata hash when stream metadata exists`() {
        val objectMapper = ObjectMapper()
        val redis = FakeStateStoreRedisCommands()
        val store = RedisCoordinatorStateStore(
            redisCommands = redis,
            objectMapper = objectMapper,
            properties = properties,
        )
        assertTrue(store.putStreamIfAbsent(streamMetadata("heartbeat-corrupt")))
        val key = GroupKey("heartbeat-corrupt", "orders-consumer")
        val keys = RedisCoordinatorStateKeys(properties.store.keyPrefix).forGroup(key)
        redis.hashes[keys.metadata] = mutableMapOf(
            "aggregate" to "{}",
            "revision" to "1",
            "schemaVersion" to "1",
            "layoutVersion" to "1",
            "updatedAt" to Instant.now(clock).toString(),
        )

        val response = service(store).heartbeat(
            streamPrefix = "heartbeat-corrupt",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = 0),
        )

        assertEquals(HeartbeatStatus.OK, response.status)
        val recovered = assertNotNull(store.get(key))
        assertEquals("heartbeat-corrupt", recovered.streamPrefix)
        assertEquals("orders-consumer", recovered.consumerGroup)
        assertEquals(1, recovered.members.size)
    }

    @Test
    fun `redis store normalizes kotlin empty collection singletons before writing group metadata`() {
        val objectMapper = KotlinEmptyCollectionRejectingObjectMapper()
        val redis = FakeStateStoreRedisCommands()
        val store = RedisCoordinatorStateStore(
            redisCommands = redis,
            objectMapper = objectMapper,
            properties = properties,
        )
        val key = GroupKey("native-empty-collections", "orders-consumer")
        val group = groupMetadataWithEmptyCollectionMember(key)

        store.save(key, group)

        val metadataKey = RedisCoordinatorStateKeys(properties.store.keyPrefix).forGroup(key).metadata
        val raw = redis.hashes.getValue(metadataKey).getValue("aggregate")
        assertTrue(raw.contains("\"currentAssignment\":[]"))
        assertTrue(raw.contains("\"grantedAssignment\":[]"))
        assertTrue(raw.contains("\"revoking\":[]"))
        assertTrue(raw.contains("\"shardProgress\":[]"))
    }

    @Test
    fun `jdbc store normalizes kotlin empty collection singletons before writing group metadata`() {
        val dataSource = DriverManagerDataSource().apply {
            setDriverClassName("org.h2.Driver")
            url = "jdbc:h2:mem:native-empty-collections-${System.nanoTime()};MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
        }
        val store = JdbcCoordinatorStateStore(JdbcTemplate(dataSource), KotlinEmptyCollectionRejectingObjectMapper())
        val key = GroupKey("jdbc-native-empty-collections", "orders-consumer")

        assertTrue(store.putIfAbsent(key, groupMetadataWithEmptyCollectionMember(key)))

        val stored = assertNotNull(store.get(key))
        val member = stored.members.getValue("member-a")
        assertTrue(member.currentAssignment.isEmpty())
        assertTrue(member.grantedAssignment.isEmpty())
        assertTrue(member.revoking.isEmpty())
        assertTrue(member.shardProgress.isEmpty())
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

    private fun groupMetadataWithEmptyCollectionMember(key: GroupKey): GroupMetadata =
        groupMetadata(key).also { group ->
            group.members["member-a"] = MemberMetadata(
                memberId = "member-a",
                memberName = "member-a",
                state = MemberState.ACTIVE,
                memberEpoch = 1,
                metadataVersion = 1,
                runtimeMaxConcurrency = 1,
                activeConsumerWorkers = 0,
                currentAssignment = emptySet(),
                grantedAssignment = emptySet(),
                revoking = emptySet(),
                lastHeartbeatAt = Instant.now(clock),
                memberLeaseExpiresAt = Instant.now(clock).plusSeconds(15),
                shardProgress = emptyList(),
            )
        }

    private fun streamMetadata(streamPrefix: String): StreamMetadata =
        StreamMetadata(
            streamPrefix = streamPrefix,
            metadataVersion = 1,
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

private class KotlinEmptyCollectionRejectingObjectMapper : ObjectMapper() {
    override fun writeValueAsString(value: Any?): String {
        check(!containsKotlinEmptyCollection(value, IdentityHashMap())) {
            "Kotlin empty collection singletons must be normalized before Redis metadata serialization"
        }
        return super.writeValueAsString(value)
    }

    private fun containsKotlinEmptyCollection(value: Any?, visited: IdentityHashMap<Any, Boolean>): Boolean {
        if (value == null) return false
        if (
            value::class.qualifiedName in setOf(
                "kotlin.collections.EmptySet",
                "kotlin.collections.EmptyList",
                "kotlin.collections.EmptyMap",
            )
        ) {
            return true
        }
        if (visited.put(value, true) != null) return false
        return when (value) {
            is GroupMetadata ->
                containsKotlinEmptyCollection(value.members, visited) ||
                    containsKotlinEmptyCollection(value.targetAssignments, visited) ||
                    containsKotlinEmptyCollection(value.migrations, visited) ||
                    containsKotlinEmptyCollection(value.metadataCorrection, visited)
            is MemberMetadata ->
                containsKotlinEmptyCollection(value.currentAssignment, visited) ||
                    containsKotlinEmptyCollection(value.grantedAssignment, visited) ||
                    containsKotlinEmptyCollection(value.revoking, visited) ||
                    containsKotlinEmptyCollection(value.shardProgress, visited)
            is MetadataCorrection -> containsKotlinEmptyCollection(value.acknowledgedMembers, visited)
            is Map<*, *> ->
                value.keys.any { containsKotlinEmptyCollection(it, visited) } ||
                    value.values.any { containsKotlinEmptyCollection(it, visited) }
            is Iterable<*> -> value.any { containsKotlinEmptyCollection(it, visited) }
            else -> false
        }
    }
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
