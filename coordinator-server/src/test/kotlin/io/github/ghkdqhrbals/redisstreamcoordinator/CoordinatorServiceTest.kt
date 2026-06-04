package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.api.*
import io.github.ghkdqhrbals.redisstreamcoordinator.config.*
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.*
import io.github.ghkdqhrbals.redisstreamcoordinator.protocol.CoordinatorProtocol
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.RedisStreamGroupInfo
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.RedisStreamInfo
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.RedisStreamRecord
import io.github.ghkdqhrbals.redisstreamcoordinator.service.CoordinatorService
import io.github.ghkdqhrbals.redisstreamcoordinator.store.*
import io.github.ghkdqhrbals.redisstreamcoordinator.stream.*

import org.mockito.Mockito
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.data.redis.connection.RedisConnectionFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import java.time.ZoneId
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicInteger

class CoordinatorServiceTest {
    private val clock = MutableClock(Instant.parse("2026-05-21T00:00:00Z"), ZoneOffset.UTC)
    private val service = service(clock)

    @Test
    fun `group rejects duplicate create`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest())

        val error = kotlin.runCatching {
            service.createGroup("orders", "orders-consumer", createGroupRequest())
        }.exceptionOrNull() as CoordinatorException

        assertEquals(CoordinatorError.GROUP_ALREADY_EXISTS, error.error)
        assertEquals(CoordinatorError.GROUP_ALREADY_EXISTS.code, error.errorCode)
    }

    @Test
    fun `first group create rejects pre-existing Redis stream prefix keys`() {
        val redis = FakeExistingKeyRedisCommands(setOf("existing-prefix:0"))
        val service = service(
            clock = clock,
            properties = CoordinatorProperties(
                streams = CoordinatorProperties.Streams(provisioningEnabled = true),
                defaults = CoordinatorProperties.Defaults(initialShardCount = 2, consumerMaxConcurrency = 4),
            ),
            redisCommands = redis,
        )

        val error = kotlin.runCatching {
            service.createGroup("existing-prefix", "orders-consumer", createGroupRequest(initialShardCount = 2))
        }.exceptionOrNull() as CoordinatorException

        assertEquals(CoordinatorError.STREAM_PREFIX_ALREADY_EXISTS, error.error)
        assertTrue(error.message.orEmpty().contains("existing-prefix:0"))
    }

    @Test
    fun `create stream validates invalid stream prefix before storing metadata`() {
        val error = kotlin.runCatching {
            service.createStream("{bad}", CreateStreamRequest(initialShardCount = 2, requestedBy = "test"))
        }.exceptionOrNull() as IllegalArgumentException

        assertEquals("streamPrefix must not contain Redis Cluster hash tag braces", error.message)
    }

    @Test
    fun `additional group create under managed prefix does not reject existing coordinator shard keys`() {
        val redis = FakeExistingKeyRedisCommands(mutableSetOf<String>())
        val service = service(
            clock = clock,
            properties = CoordinatorProperties(
                streams = CoordinatorProperties.Streams(provisioningEnabled = true),
                defaults = CoordinatorProperties.Defaults(initialShardCount = 2, consumerMaxConcurrency = 4),
            ),
            redisCommands = redis,
        )

        service.createGroup("managed-prefix", "orders-consumer", createGroupRequest(initialShardCount = 2))
        redis.existingKeys += "managed-prefix:0"
        redis.existingKeys += "managed-prefix:1"

        val response = service.createGroup("managed-prefix", "analytics-consumer", createGroupRequest(initialShardCount = 2))

        assertEquals("analytics-consumer", response.consumerGroup)
    }

    @Test
    fun `delete group removes inactive metadata`() {
        service.createGroup("delete-orders", "orders-consumer", createGroupRequest())

        val deleted = service.deleteGroup(
            "delete-orders",
            "orders-consumer",
            DeleteGroupRequest(
                requestedBy = "test",
                reason = "cleanup inactive test group",
            ),
        )

        assertEquals("delete-orders", deleted.streamPrefix)
        val error = kotlin.runCatching {
            service.getGroup("delete-orders", "orders-consumer")
        }.exceptionOrNull() as CoordinatorException
        assertEquals(CoordinatorError.GROUP_NOT_FOUND, error.error)
    }

    @Test
    fun `delete group rejects live members unless forced`() {
        service.createGroup("delete-live-orders", "orders-consumer", createGroupRequest())
        service.heartbeat(
            "delete-live-orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )

        val rejected = kotlin.runCatching {
            service.deleteGroup(
                "delete-live-orders",
                "orders-consumer",
                DeleteGroupRequest(
                    requestedBy = "test",
                    reason = "cleanup live test group",
                ),
            )
        }.exceptionOrNull() as CoordinatorException

        assertEquals(CoordinatorError.GROUP_HAS_ACTIVE_MEMBERS, rejected.error)
        service.deleteGroup(
            "delete-live-orders",
            "orders-consumer",
            DeleteGroupRequest(
                requestedBy = "test",
                reason = "forced cleanup live test group",
                force = true,
            ),
        )
        val notFound = kotlin.runCatching {
            service.getGroup("delete-live-orders", "orders-consumer")
        }.exceptionOrNull() as CoordinatorException
        assertEquals(CoordinatorError.GROUP_NOT_FOUND, notFound.error)
    }

    @Test
    fun `grafana option APIs expose current sharded stream selections from metadata`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest(initialShardCount = 3))
        service.createGroup("payments", "payments-consumer", createGroupRequest(initialShardCount = 2))

        assertEquals(
            listOf(
                GrafanaOptionRow(text = "orders", value = "orders"),
                GrafanaOptionRow(text = "payments", value = "payments"),
            ),
            service.grafanaStreamOptions(),
        )
        assertEquals(
            listOf(GrafanaOptionRow(text = "orders / orders-consumer", value = "orders-consumer")),
            service.grafanaConsumerGroupOptions("orders"),
        )
        assertEquals(
            listOf(
                GrafanaOptionRow(text = ":0", value = "0"),
                GrafanaOptionRow(text = ":1", value = "1"),
                GrafanaOptionRow(text = ":2", value = "2"),
            ),
            service.grafanaShardOptions("orders", "orders-consumer"),
        )
    }

    @Test
    fun `grafana shard rows can scan all groups for overview dashboards`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest(initialShardCount = 3))
        service.createGroup("payments", "payments-consumer", createGroupRequest(initialShardCount = 2))

        val rows = service.grafanaShards(streamPrefix = "", consumerGroup = "")

        assertEquals(5, rows.size)
        assertEquals(
            setOf("orders" to "orders-consumer", "payments" to "payments-consumer"),
            rows.map { it.streamPrefix to it.consumerGroup }.toSet(),
        )
        assertEquals(setOf(":0", ":1", ":2"), rows.filter { it.streamPrefix == "orders" }.map { it.shardLabel }.toSet())
        assertEquals(setOf(":0", ":1"), rows.filter { it.streamPrefix == "payments" }.map { it.shardLabel }.toSet())
    }

    @Test
    fun `grafana message search finds a record id across every shard`() {
        val redis = FakeMessageRedisCommands(
            mapOf(
                "search-orders:0" to listOf(
                    RedisStreamRecord("100-1", mapOf("payload" to """{"shard":0}""")),
                ),
                "search-orders:1" to listOf(
                    RedisStreamRecord("100-2", mapOf("payload" to """{"shard":1}""")),
                ),
                "search-orders:2" to listOf(
                    RedisStreamRecord("100-1", mapOf("payload" to """{"shard":2}""")),
                ),
            ),
        )
        val service = service(clock, redisCommands = redis)
        service.createGroup("search-orders", "orders-consumer", createGroupRequest(initialShardCount = 3))

        val rows = service.grafanaMessages(
            streamPrefix = "search-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "0",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = null,
            recordId = "100-1",
            limit = 25,
        )

        assertEquals(listOf(0, 2), rows.map { it.shardIndex })
        assertEquals(listOf("search-orders:0", "search-orders:2"), rows.map { it.streamKey })
        assertEquals(listOf("search-orders:0", "search-orders:1", "search-orders:2"), redis.rangeRequests)
        assertEquals("all", rows.first().shardSelector)
        assertEquals(100L, rows.first().recordTimestampMs)
        assertEquals(Instant.ofEpochMilli(100), rows.first().recordTime)
        assertEquals(null, rows.first().pageNextCursor)
    }

    @Test
    fun `grafana messages can page backward through a single shard more than once`() {
        val redis = FakeMessageRedisCommands(
            mapOf(
                "paged-orders:0" to listOf(
                    RedisStreamRecord("100-0", mapOf("payload" to "one")),
                    RedisStreamRecord("101-0", mapOf("payload" to "two")),
                    RedisStreamRecord("102-0", mapOf("payload" to "three")),
                    RedisStreamRecord("103-0", mapOf("payload" to "four")),
                    RedisStreamRecord("104-0", mapOf("payload" to "five")),
                ),
            ),
        )
        val service = service(clock, redisCommands = redis)
        service.createGroup("paged-orders", "orders-consumer", createGroupRequest(initialShardCount = 1))

        val firstPage = service.grafanaMessages(
            streamPrefix = "paged-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "0",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = null,
            recordId = null,
            limit = 2,
        )
        val secondPage = service.grafanaMessages(
            streamPrefix = "paged-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "0",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = firstPage.first().pageNextCursor,
            recordId = null,
            limit = 2,
        )
        val thirdPage = service.grafanaMessages(
            streamPrefix = "paged-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "0",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = secondPage.first().pageNextCursor,
            recordId = null,
            limit = 2,
        )

        assertEquals(listOf("104-0", "103-0"), firstPage.map { it.recordId })
        assertEquals("103-0", firstPage.first().pageNextCursor)
        assertEquals(listOf("102-0", "101-0"), secondPage.map { it.recordId })
        assertEquals("101-0", secondPage.first().pageNextCursor)
        assertEquals(listOf("100-0"), thirdPage.map { it.recordId })
        assertEquals(null, thirdPage.first().pageNextCursor)
    }

    @Test
    fun `grafana messages can jump to the last page for a single shard`() {
        val redis = FakeMessageRedisCommands(
            mapOf(
                "last-page-orders:0" to listOf(
                    RedisStreamRecord("100-0", mapOf("payload" to "one")),
                    RedisStreamRecord("101-0", mapOf("payload" to "two")),
                    RedisStreamRecord("102-0", mapOf("payload" to "three")),
                    RedisStreamRecord("103-0", mapOf("payload" to "four")),
                    RedisStreamRecord("104-0", mapOf("payload" to "five")),
                ),
            ),
        )
        val service = service(clock, redisCommands = redis)
        service.createGroup("last-page-orders", "orders-consumer", createGroupRequest(initialShardCount = 1))

        val newestFirst = service.grafanaMessages(
            streamPrefix = "last-page-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "0",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = "__rsc_last__",
            recordId = null,
            limit = 2,
        )
        val oldestFirst = service.grafanaMessages(
            streamPrefix = "last-page-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "0",
            direction = StreamMessagePageDirection.FORWARD,
            cursor = "__rsc_last__",
            recordId = null,
            limit = 2,
        )

        assertEquals(listOf("100-0"), newestFirst.map { it.recordId })
        assertEquals(listOf("104-0"), oldestFirst.map { it.recordId })
        assertEquals(null, newestFirst.first().pageNextCursor)
        assertEquals(null, oldestFirst.first().pageNextCursor)

        val pageBeforeLast = service.grafanaMessages(
            streamPrefix = "last-page-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "0",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = "__rsc_tail__:1",
            recordId = null,
            limit = 2,
        )

        assertEquals(listOf("102-0", "101-0"), pageBeforeLast.map { it.recordId })
        assertEquals("__rsc_tail__:0", pageBeforeLast.first().pageNextCursor)
    }

    @Test
    fun `grafana messages can jump to the last page across all shards`() {
        val redis = FakeMessageRedisCommands(
            mapOf(
                "last-page-all-orders:0" to listOf(
                    RedisStreamRecord("100-0", mapOf("payload" to "s0-a")),
                    RedisStreamRecord("105-0", mapOf("payload" to "s0-b")),
                    RedisStreamRecord("110-0", mapOf("payload" to "s0-c")),
                ),
                "last-page-all-orders:1" to listOf(
                    RedisStreamRecord("101-0", mapOf("payload" to "s1-a")),
                    RedisStreamRecord("106-0", mapOf("payload" to "s1-b")),
                    RedisStreamRecord("111-0", mapOf("payload" to "s1-c")),
                ),
            ),
        )
        val service = service(clock, redisCommands = redis)
        service.createGroup("last-page-all-orders", "orders-consumer", createGroupRequest(initialShardCount = 2))

        val rows = service.grafanaMessages(
            streamPrefix = "last-page-all-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "all",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = "__rsc_last__",
            recordId = null,
            limit = 4,
        )

        assertEquals(listOf("101-0", "100-0"), rows.map { it.recordId })
        assertEquals(listOf(1, 0), rows.map { it.shardIndex })
        assertEquals("all", rows.first().shardSelector)
        assertEquals(null, rows.first().pageNextCursor)
        assertEquals(6, rows.first().pageTotalMessages)

        val pageBeforeLast = service.grafanaMessages(
            streamPrefix = "last-page-all-orders",
            consumerGroup = "orders-consumer",
            shardIndex = "all",
            direction = StreamMessagePageDirection.BACKWARD,
            cursor = "__rsc_tail__:1",
            recordId = null,
            limit = 4,
        )

        assertEquals(listOf("111-0", "110-0", "106-0", "105-0"), pageBeforeLast.map { it.recordId })
        assertEquals(listOf(1, 0, 1, 0), pageBeforeLast.map { it.shardIndex })
        assertEquals("__rsc_tail__:0", pageBeforeLast.first().pageNextCursor)
    }

    @Test
    fun `grafana shard rows expose target owners separately from acknowledged current owners`() {
        service.createGroup("owner-pending", "orders-consumer", createGroupRequest(initialShardCount = 2))
        service.heartbeat(
            "owner-pending",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )

        val rows = service.grafanaShards("owner-pending", "orders-consumer")

        assertEquals(setOf("member-a"), rows.map { it.targetOwnerMemberIds }.toSet())
        assertEquals(setOf(""), rows.map { it.currentOwnerMemberIds }.toSet())
        assertEquals(setOf("PENDING_ACK"), rows.map { it.ownerState }.toSet())
    }

    @Test
    fun `grafana shard rows expose produced and consumed rates after repeated observations`() {
        val redis = FakeOffsetRedisCommands()
        redis.setShard(streamKey = "rate-orders:0", length = 100, lag = 20)
        val service = service(
            clock,
            redisCommands = redis,
            properties = CoordinatorProperties(
                monitoring = CoordinatorProperties.Monitoring(offsetCacheTtlMs = 0),
            ),
        )
        service.createGroup("rate-orders", "orders-consumer", createGroupRequest(initialShardCount = 1))

        val first = service.grafanaShards("rate-orders", "orders-consumer").single()

        clock.advance(Duration.ofSeconds(20))
        redis.setShard(streamKey = "rate-orders:0", length = 160, lag = 35)
        val second = service.grafanaShards("rate-orders", "orders-consumer").single()

        assertEquals(null, first.producedPerSecond)
        assertEquals(null, first.consumedPerSecond)
        assertEquals(3.0, second.producedPerSecond)
        assertEquals(2.25, second.consumedPerSecond)
    }

    @Test
    fun `grafana group rows aggregate produced and consumed rates`() {
        val redis = FakeOffsetRedisCommands()
        redis.setShard(streamKey = "rate-group-orders:0", length = 100, lag = 20)
        redis.setShard(streamKey = "rate-group-orders:1", length = 200, lag = 30)
        val service = service(
            clock,
            redisCommands = redis,
            properties = CoordinatorProperties(
                monitoring = CoordinatorProperties.Monitoring(offsetCacheTtlMs = 0),
            ),
        )
        service.createGroup("rate-group-orders", "orders-consumer", createGroupRequest(initialShardCount = 2))

        service.grafanaGroups().single { it.streamPrefix == "rate-group-orders" }
        clock.advance(Duration.ofSeconds(20))
        redis.setShard(streamKey = "rate-group-orders:0", length = 150, lag = 30)
        redis.setShard(streamKey = "rate-group-orders:1", length = 260, lag = 20)
        val row = service.grafanaGroups().single { it.streamPrefix == "rate-group-orders" }

        assertEquals(5.5, row.producedPerSecond)
        assertEquals(5.5, row.consumedPerSecond)
    }

    @Test
    fun `grafana offset cache reuses shard reads across overview panels`() {
        val redis = FakeOffsetRedisCommands()
        repeat(4) { shard ->
            redis.setShard(streamKey = "cache-orders:$shard", length = 100 + shard.toLong(), lag = shard.toLong())
        }
        val service = service(clock, redisCommands = redis)
        service.createGroup("cache-orders", "orders-consumer", createGroupRequest(initialShardCount = 4))

        service.grafanaGroups().single { it.streamPrefix == "cache-orders" }
        val readsAfterGroups = redis.xInfoStreamCalls.get()
        service.grafanaShards("cache-orders", "orders-consumer")

        assertEquals(4, readsAfterGroups)
        assertEquals(readsAfterGroups, redis.xInfoStreamCalls.get())
    }

    @Test
    fun `grafana offset cache is not invalidated by heartbeat metadata changes`() {
        val redis = FakeOffsetRedisCommands()
        repeat(2) { shard ->
            redis.setShard(streamKey = "heartbeat-cache-orders:$shard", length = 100 + shard.toLong(), lag = 0)
        }
        val service = service(clock, redisCommands = redis)
        service.createGroup("heartbeat-cache-orders", "orders-consumer", createGroupRequest(initialShardCount = 2))

        service.grafanaShards("heartbeat-cache-orders", "orders-consumer")
        val readsAfterInitialSnapshot = redis.xInfoStreamCalls.get()
        service.heartbeat(
            "heartbeat-cache-orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.grafanaShards("heartbeat-cache-orders", "orders-consumer")

        assertEquals(readsAfterInitialSnapshot, redis.xInfoStreamCalls.get())
    }

    @Test
    fun `grafana shard offset reads can run in parallel`() {
        val redis = FakeOffsetRedisCommands(delayMs = 25)
        repeat(8) { shard ->
            redis.setShard(streamKey = "parallel-orders:$shard", length = 100 + shard.toLong(), lag = 0)
        }
        val service = service(
            clock = clock,
            redisCommands = redis,
            properties = CoordinatorProperties(
                heartbeatInterval = Duration.ofSeconds(3),
                memberLeaseTtl = Duration.ofSeconds(15),
                defaults = CoordinatorProperties.Defaults(initialShardCount = 8, consumerMaxConcurrency = 8),
                monitoring = CoordinatorProperties.Monitoring(shardQueryParallelism = 4),
            ),
        )
        service.createGroup("parallel-orders", "orders-consumer", createGroupRequest(initialShardCount = 8))

        service.grafanaShards("parallel-orders", "orders-consumer")

        assertTrue(redis.maxConcurrentReads.get() > 1)
    }

    @Test
    fun `grafana overview scans multiple groups in parallel`() {
        val redis = FakeOffsetRedisCommands(delayMs = 25)
        redis.setShard(streamKey = "parallel-orders:0", length = 100, lag = 0)
        redis.setShard(streamKey = "parallel-payments:0", length = 100, lag = 0)
        redis.setShard(streamKey = "parallel-refunds:0", length = 100, lag = 0)
        val service = service(
            clock = clock,
            redisCommands = redis,
            properties = CoordinatorProperties(
                heartbeatInterval = Duration.ofSeconds(3),
                memberLeaseTtl = Duration.ofSeconds(15),
                defaults = CoordinatorProperties.Defaults(initialShardCount = 1, consumerMaxConcurrency = 1),
                monitoring = CoordinatorProperties.Monitoring(
                    groupQueryParallelism = 3,
                    shardQueryParallelism = 3,
                ),
            ),
        )
        service.createGroup("parallel-orders", "orders-consumer", createGroupRequest(initialShardCount = 1))
        service.createGroup("parallel-payments", "orders-consumer", createGroupRequest(initialShardCount = 1))
        service.createGroup("parallel-refunds", "orders-consumer", createGroupRequest(initialShardCount = 1))

        val rows = service.grafanaShards("", "")

        assertEquals(3, rows.size)
        assertTrue(redis.maxConcurrentReads.get() > 1)
    }

    @Test
    fun `heartbeat accepts previously granted ownership after target changes before ack`() {
        service.createGroup("granted-before-ack", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "granted-before-ack",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "granted-before-ack",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )

        val acknowledgedA = service.heartbeat(
            "granted-before-ack",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = memberA.assignment.assignedShards,
            ),
        )
        val member = service.listMembers("granted-before-ack", "orders-consumer").members
            .single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.OK, acknowledgedA.status)
        assertEquals(MemberState.ACTIVE, member.state)
        assertEquals(memberA.assignment.assignedShards, member.currentAssignment)
    }

    @Test
    fun `monitoring live owner rows exclude expired members with stale current assignments`() {
        service.createGroup("owner-monitoring", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "owner-monitoring",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "owner-monitoring",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        val memberB = service.heartbeat(
            "owner-monitoring",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        service.heartbeat(
            "owner-monitoring",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch, ownedShards = memberB.assignment.assignedShards),
        )

        val offsets = service.streamShardOffsets("owner-monitoring", "orders-consumer")
        val assignmentRows = service.grafanaAssignments("owner-monitoring", "orders-consumer")

        assertEquals(setOf("member-b"), offsets.shards.flatMap { it.ownerMemberIds }.toSet())
        assertTrue(offsets.shards.none { "member-a" in it.ownerMemberIds })
        assertEquals(setOf("member-b"), assignmentRows.map { it.currentOwners }.toSet())
    }

    @Test
    fun `terminal members are pruned after stale member retention`() {
        val clock = MutableClock(Instant.parse("2026-05-21T00:00:00Z"), ZoneOffset.UTC)
        val service = service(
            clock = clock,
            properties = CoordinatorProperties(
                heartbeatInterval = Duration.ofSeconds(3),
                memberLeaseTtl = Duration.ofSeconds(15),
                staleMemberRetention = Duration.ofSeconds(30),
                defaults = CoordinatorProperties.Defaults(
                    initialShardCount = 2,
                    consumerMaxConcurrency = 2,
                ),
            ),
        )
        service.createGroup("member-prune", "orders-consumer", createGroupRequest(initialShardCount = 2))
        service.heartbeat("member-prune", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))

        clock.advance(Duration.ofSeconds(16))
        service.tick()

        assertEquals(MemberState.EXPIRED, service.listMembers("member-prune", "orders-consumer").members.single().state)

        clock.advance(Duration.ofSeconds(15))
        service.tick()

        assertTrue(service.listMembers("member-prune", "orders-consumer").members.isEmpty())
    }

    @Test
    fun `heartbeat rejects member id mismatch`() {
        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-b", memberEpoch = 0),
        )

        assertEquals(HeartbeatStatus.INVALID_REQUEST, response.status)
    }

    @Test
    fun `heartbeat returns unknown member for missing group`() {
        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = 1),
        )

        assertEquals(HeartbeatStatus.UNKNOWN_MEMBER_ID, response.status)
    }

    @Test
    fun `initial join heartbeat replay returns current assignment without changing epochs`() {
        service.createGroup("join-idempotent", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val request = heartbeat("member-a", memberEpoch = 0)

        val first = service.heartbeat("join-idempotent", "orders-consumer", "member-a", request)
        val replayed = service.heartbeat("join-idempotent", "orders-consumer", "member-a", request)
        val group = service.getGroup("join-idempotent", "orders-consumer")
        val member = service.listMembers("join-idempotent", "orders-consumer").members.single()

        assertEquals(HeartbeatStatus.OK, first.status)
        assertEquals(HeartbeatStatus.OK, replayed.status)
        assertEquals(first.memberEpoch, replayed.memberEpoch)
        assertEquals(first.groupEpoch, replayed.groupEpoch)
        assertEquals(first.assignmentEpoch, replayed.assignmentEpoch)
        assertEquals(first.metadataVersion, replayed.metadataVersion)
        assertEquals(first.assignment.assignedShards, replayed.assignment.assignedShards)
        assertEquals(first.metadataVersion, group.metadataVersion)
        assertEquals(first.groupEpoch, group.groupEpoch)
        assertEquals(first.assignmentEpoch, group.assignmentEpoch)
        assertEquals(first.metadataVersion, member.metadataVersion)
        assertTrue(member.currentAssignment.isEmpty())
    }

    @Test
    fun `steady heartbeat replay does not bump metadata or assignment epochs`() {
        service.createGroup("steady-idempotent", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val joined = service.heartbeat(
            "steady-idempotent",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val request = heartbeat(
            "member-a",
            memberEpoch = joined.memberEpoch,
            ownedShards = joined.assignment.assignedShards,
        )

        val first = service.heartbeat("steady-idempotent", "orders-consumer", "member-a", request)
        val replayed = service.heartbeat("steady-idempotent", "orders-consumer", "member-a", request)
        val group = service.getGroup("steady-idempotent", "orders-consumer")

        assertEquals(HeartbeatStatus.OK, first.status)
        assertEquals(HeartbeatStatus.OK, replayed.status)
        assertEquals(first.memberEpoch, replayed.memberEpoch)
        assertEquals(first.groupEpoch, replayed.groupEpoch)
        assertEquals(first.assignmentEpoch, replayed.assignmentEpoch)
        assertEquals(first.metadataVersion, replayed.metadataVersion)
        assertEquals(first.metadataVersion, group.metadataVersion)
        assertEquals(first.assignment.assignedShards, replayed.assignment.assignedShards)
    }

    @Test
    fun `coordination version is accepted only inside module support range`() {
        val service = service(clock = clock)
        service.createGroup("protocol", "orders-consumer", createGroupRequest())

        val accepted = service.heartbeat(
            "protocol",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = 0,
                protocolVersion = CoordinatorProtocol.CURRENT_COORDINATION_VERSION,
            ),
        )
        val rejectedBelowMinimum = service.heartbeat(
            "protocol",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = 0,
                protocolVersion = CoordinatorProtocol.MIN_COORDINATION_VERSION - 1,
            ),
        )
        val rejectedAboveMaximum = service.heartbeat(
            "protocol",
            "orders-consumer",
            "member-c",
            heartbeat(
                "member-c",
                memberEpoch = 0,
                protocolVersion = CoordinatorProtocol.MAX_COORDINATION_VERSION + 1,
            ),
        )

        assertEquals(HeartbeatStatus.OK, accepted.status)
        assertEquals(HeartbeatStatus.UNSUPPORTED_PROTOCOL, rejectedBelowMinimum.status)
        assertEquals(HeartbeatStatus.UNSUPPORTED_PROTOCOL, rejectedAboveMaximum.status)
    }

    @Test
    fun `health skips redis ping when redis features are disabled`() {
        val redisConnectionFactory = Mockito.mock(RedisConnectionFactory::class.java)
        val service = CoordinatorService(
            properties = CoordinatorProperties(),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = redisProvider(redisConnectionFactory),
            clock = clock,
        )

        val health = service.health()

        assertEquals("UP", health.status)
        assertEquals("NOT_CONFIGURED", health.redis)
        Mockito.verifyNoInteractions(redisConnectionFactory)
    }

    @Test
    fun `health degrades when redis is required and unavailable`() {
        val redisConnectionFactory = Mockito.mock(RedisConnectionFactory::class.java)
        Mockito.`when`(redisConnectionFactory.connection).thenThrow(IllegalStateException("redis down"))
        val service = CoordinatorService(
            properties = CoordinatorProperties(store = CoordinatorProperties.Store(type = CoordinatorProperties.StoreType.REDIS)),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = redisProvider(redisConnectionFactory),
            clock = clock,
        )

        val first = service.health()
        waitUntil {
            service.health().redis == "DOWN"
        }
        val health = service.health()

        assertEquals("UP", first.status)
        assertEquals("UNKNOWN", first.redis)
        assertEquals("DEGRADED", health.status)
        assertEquals("DOWN", health.redis)
        Mockito.verify(redisConnectionFactory, Mockito.atLeastOnce()).connection
    }

    @Test
    fun `health refreshes redis status asynchronously and reuses cache within configured ttl`() {
        val redisConnection = Mockito.mock(org.springframework.data.redis.connection.RedisConnection::class.java)
        Mockito.`when`(redisConnection.ping()).thenReturn("PONG")
        val redisConnectionFactory = Mockito.mock(RedisConnectionFactory::class.java)
        Mockito.`when`(redisConnectionFactory.connection).thenReturn(redisConnection)
        val service = CoordinatorService(
            properties = CoordinatorProperties(
                store = CoordinatorProperties.Store(type = CoordinatorProperties.StoreType.REDIS),
                health = CoordinatorProperties.Health(redisTimeoutMs = 100, cacheTtlMs = 10_000),
            ),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = redisProvider(redisConnectionFactory),
            clock = clock,
        )

        val first = service.health()
        waitUntil {
            service.health().redis == "UP"
        }
        val second = service.health()
        val third = service.health()

        assertEquals("UP", first.status)
        assertEquals("UNKNOWN", first.redis)
        assertEquals("UP", second.status)
        assertEquals("UP", second.redis)
        assertEquals("UP", third.redis)
        Mockito.verify(redisConnectionFactory, Mockito.times(1)).connection
        Mockito.verify(redisConnection, Mockito.times(1)).ping()
    }

    @Test
    fun `health does not wait for a slow redis ping beyond configured timeout`() {
        val redisConnectionFactory = Mockito.mock(RedisConnectionFactory::class.java)
        Mockito.`when`(redisConnectionFactory.connection).thenAnswer {
            Thread.sleep(250)
            throw IllegalStateException("redis ping timed out")
        }
        val service = CoordinatorService(
            properties = CoordinatorProperties(
                store = CoordinatorProperties.Store(type = CoordinatorProperties.StoreType.REDIS),
                health = CoordinatorProperties.Health(redisTimeoutMs = 10),
            ),
            stateStore = InMemoryCoordinatorStateStore(),
            redisConnectionFactory = redisProvider(redisConnectionFactory),
            clock = clock,
        )

        val startedAt = System.nanoTime()
        val health = service.health()
        val elapsedMs = (System.nanoTime() - startedAt) / 1_000_000

        assertEquals("UP", health.status)
        assertEquals("UNKNOWN", health.redis)
        assertTrue(elapsedMs < 50, "health took ${elapsedMs}ms despite async Redis refresh")
        waitUntil {
            service.health().redis == "DOWN"
        }
    }

    @Test
    fun `heartbeat rejects unknown member leave`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest())

        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = -1),
        )

        assertEquals(HeartbeatStatus.UNKNOWN_MEMBER_ID, response.status)
        assertTrue(service.listMembers("orders", "orders-consumer").members.isEmpty())
    }

    @Test
    fun `membership expires owner and reassigns shards`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("orders", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        clock.advance(Duration.ofSeconds(16))
        val second = service.heartbeat("orders", "orders-consumer", "member-b", heartbeat("member-b", memberEpoch = 0))
        val assignments = service.assignments("orders", "orders-consumer")

        assertEquals(setOf(ShardId(0), ShardId(1)), assignments.targetAssignment.getValue("member-b"))
        assertEquals(setOf(ShardId(0), ShardId(1)), second.assignment.assignedShards)
        assertTrue(second.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `coordinator tick expires silent members without waiting for another heartbeat`() {
        service.createGroup("tick-orders", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("tick-orders", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "tick-orders",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        clock.advance(Duration.ofSeconds(16))
        val tick = service.tick()
        val members = service.listMembers("tick-orders", "orders-consumer").members
        val assignments = service.assignments("tick-orders", "orders-consumer")

        assertEquals(1, tick.scannedGroups)
        assertEquals(1, tick.changedGroups)
        assertEquals(MemberState.EXPIRED, members.single().state)
        assertTrue(assignments.targetAssignment.isEmpty())
    }

    @Test
    fun `monitoring read does not acquire critical section or write refreshed state`() {
        val store = CopyingConflictOnceStateStore()
        val service = service(clock, store)
        service.createGroup("monitor-race", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("monitor-race", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "monitor-race",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        clock.advance(Duration.ofSeconds(16))
        store.conflictsBeforeSave = 1
        val group = service.getGroup("monitor-race", "orders-consumer")

        assertEquals(GroupState.STABLE, group.state)
        assertEquals(0, store.conflictedSaves)
        assertEquals(MemberState.ACTIVE, service.listMembers("monitor-race", "orders-consumer").members.single().state)

        service.tick()

        assertEquals(1, store.conflictedSaves)
        assertEquals(MemberState.EXPIRED, service.listMembers("monitor-race", "orders-consumer").members.single().state)
    }

    @Test
    fun `logical members receive even shard assignments`() {
        service.createGroup("events", "events-consumer", createGroupRequest(initialShardCount = 8))
        (0 until 4).forEach { index ->
            service.heartbeat("events", "events-consumer", "pod-a-m$index", heartbeat("pod-a-m$index", memberEpoch = 0))
        }
        val assignments = service.assignments("events", "events-consumer").targetAssignment

        assertEquals(setOf("pod-a-m0", "pod-a-m1", "pod-a-m2", "pod-a-m3"), assignments.keys)
        assertEquals(listOf(2, 2, 2, 2), assignments.values.map { it.size }.sorted())
    }

    @Test
    fun `new logical members rebalance shards by member count`() {
        service.createGroup("member-split", "events-consumer", createGroupRequest(initialShardCount = 8))
        val first = service.heartbeat("member-split", "events-consumer", "pod-a-m0", heartbeat("pod-a-m0", memberEpoch = 0))
        service.heartbeat(
            "member-split",
            "events-consumer",
            "pod-a-m0",
            heartbeat("pod-a-m0", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        service.heartbeat("member-split", "events-consumer", "pod-a-m1", heartbeat("pod-a-m1", memberEpoch = 0))
        service.heartbeat("member-split", "events-consumer", "pod-a-m2", heartbeat("pod-a-m2", memberEpoch = 0))
        service.heartbeat("member-split", "events-consumer", "pod-a-m3", heartbeat("pod-a-m3", memberEpoch = 0))

        val assignments = service.assignments("member-split", "events-consumer").targetAssignment

        assertEquals(listOf(2, 2, 2, 2), assignments.values.map { it.size }.sorted())
    }

    @Test
    fun `member split join advances assignment epochs`() {
        service.createGroup("member-split-epochs", "events-consumer", createGroupRequest(initialShardCount = 8))
        val first = service.heartbeat(
            "member-split-epochs",
            "events-consumer",
            "pod-a-m0",
            heartbeat("pod-a-m0", memberEpoch = 0),
        )
        service.heartbeat(
            "member-split-epochs",
            "events-consumer",
            "pod-a-m0",
            heartbeat("pod-a-m0", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        val before = service.getGroup("member-split-epochs", "events-consumer")
        val second = service.heartbeat(
            "member-split-epochs",
            "events-consumer",
            "pod-a-m1",
            heartbeat("pod-a-m1", memberEpoch = 0),
        )
        val after = service.getGroup("member-split-epochs", "events-consumer")
        val memberA = service.heartbeat(
            "member-split-epochs",
            "events-consumer",
            "pod-a-m0",
            heartbeat("pod-a-m0", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )
        val memberB = service.heartbeat(
            "member-split-epochs",
            "events-consumer",
            "pod-a-m1",
            heartbeat("pod-a-m1", memberEpoch = second.memberEpoch),
        )

        assertTrue(after.groupEpoch > before.groupEpoch)
        assertEquals(before.groupEpoch + 1, after.groupEpoch)
        assertEquals(after.groupEpoch, after.assignmentEpoch)
        assertEquals(after.assignmentEpoch, memberA.memberEpoch)
        assertEquals(after.assignmentEpoch, memberB.memberEpoch)
        assertEquals(4, memberA.assignment.assignedShards.size)
        assertEquals(4, memberB.assignment.pendingShards.size)
        assertTrue(memberB.assignment.assignedShards.isEmpty())
    }

    @Test
    fun `migration rollback restores previous version`() {
        service.createGroup("metrics", "metrics-consumer", createGroupRequest(initialShardCount = 2))
        val migration = service.scaleGroup(
            "metrics",
            "metrics-consumer",
            ScaleGroupRequest(targetShardCount = 4, requestedBy = "test", reason = "scale out"),
        )

        val rolledBack = service.rollbackMigration(
            "metrics",
            "metrics-consumer",
            migration.reshardingId,
        )
        val group = service.getGroup("metrics", "metrics-consumer")

        assertEquals(MigrationState.ROLLED_BACK, rolledBack.state)
                        assertEquals(null, group.activeMigration)
    }

    @Test
    fun `migration rollback rejects unknown id`() {
        service.createGroup("metrics", "metrics-consumer", createGroupRequest(initialShardCount = 2))

        val error = kotlin.runCatching {
            service.rollbackMigration("metrics", "metrics-consumer", "reshard-missing")
        }.exceptionOrNull() as CoordinatorException

        assertEquals(CoordinatorError.MIGRATION_NOT_FOUND, error.error)
        assertEquals(CoordinatorError.MIGRATION_NOT_FOUND.code, error.errorCode)
    }

    @Test
    fun `membership rejoin restores expired member`() {
        service.createGroup("logs", "logs-consumer", createGroupRequest(initialShardCount = 2))
        service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        clock.advance(Duration.ofSeconds(16))
        service.getGroup("logs", "logs-consumer")

        val rejoined = service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        val member = service.listMembers("logs", "logs-consumer").members.single()

        assertEquals(HeartbeatStatus.OK, rejoined.status)
        assertEquals(MemberState.ACTIVE, member.state)
        assertEquals(rejoined.memberEpoch, member.memberEpoch)
    }

    @Test
    fun `membership rejoin restores gracefully left member`() {
        service.createGroup("logs", "logs-consumer", createGroupRequest(initialShardCount = 2))
        val joined = service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "logs",
            "logs-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = joined.memberEpoch, ownedShards = joined.assignment.assignedShards),
        )
        service.heartbeat(
            "logs",
            "logs-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = -1,
                ownedShards = joined.assignment.assignedShards,
                revokingShards = joined.assignment.assignedShards.map {
                    RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                },
            ),
        )

        val rejoined = service.heartbeat("logs", "logs-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        val member = service.listMembers("logs", "logs-consumer").members.single()

        assertEquals(HeartbeatStatus.OK, rejoined.status)
        assertEquals(MemberState.ACTIVE, member.state)
        assertEquals(rejoined.memberEpoch, member.memberEpoch)
        assertEquals(joined.assignment.assignedShards, rejoined.assignment.assignedShards)
    }

    @Test
    fun `assignment first member gets readable shards`() {
        service.createGroup("orders", "orders-consumer", createGroupRequest())

        val response = service.heartbeat(
            streamPrefix = "orders",
            consumerGroup = "orders-consumer",
            memberId = "member-a",
            request = heartbeat("member-a", memberEpoch = 0),
        )

        assertEquals(HeartbeatStatus.OK, response.status)
        assertEquals(
            setOf(ShardId(0), ShardId(1), ShardId(2), ShardId(3)),
            response.assignment.assignedShards,
        )
        assertTrue(response.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `assignment waits for previous owner revoke`() {
        service.createGroup("payments", "payments-consumer", createGroupRequest(initialShardCount = 2))
        val first = service.heartbeat("payments", "payments-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        service.heartbeat(
            "payments",
            "payments-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = first.memberEpoch, ownedShards = first.assignment.assignedShards),
        )

        val second = service.heartbeat("payments", "payments-consumer", "member-b", heartbeat("member-b", memberEpoch = 0))

        assertEquals(setOf(ShardId(1)), second.assignment.pendingShards)
        assertTrue(second.assignment.assignedShards.isEmpty())

        val revokeFromA = service.heartbeat(
            "payments",
            "payments-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = first.memberEpoch,
                ownedShards = setOf(ShardId(0)),
                revokingShards = listOf(RevokingShardReport(ShardId(1), RevokingShardState.REVOKED)),
            ),
        )
        assertEquals(setOf(ShardId(0)), revokeFromA.assignment.assignedShards)

        val assignedToB = service.heartbeat(
            "payments",
            "payments-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = second.memberEpoch),
        )
        assertEquals(setOf(ShardId(1)), assignedToB.assignment.assignedShards)
        assertTrue(assignedToB.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `rebalance timeout fences stuck owner`() {
        val service = service(clock, rebalanceTimeout = Duration.ofSeconds(5))
        service.createGroup("rebalance-timeout", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        assertEquals(5_000, memberA.rebalanceTimeoutMs)
        service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = memberA.assignment.assignedShards,
            ),
        )
        val memberB = service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        assertEquals(setOf(ShardId(1)), memberB.assignment.pendingShards)

        clock.advance(Duration.ofSeconds(6))
        val afterTimeout = service.heartbeat(
            "rebalance-timeout",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch),
        )
        val members = service.listMembers("rebalance-timeout", "orders-consumer").members
        val assignments = service.assignments("rebalance-timeout", "orders-consumer")

        assertEquals(setOf(ShardId(0), ShardId(1)), afterTimeout.assignment.assignedShards)
        assertEquals(MemberState.FENCED, members.single { it.memberId == "member-a" }.state)
        assertEquals(MemberState.ACTIVE, members.single { it.memberId == "member-b" }.state)
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `rebalance timeout keeps timely owner active`() {
        val service = service(clock, rebalanceTimeout = Duration.ofSeconds(5))
        service.createGroup("rebalance-completes", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = memberA.assignment.assignedShards,
            ),
        )
        val memberB = service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )

        service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = setOf(ShardId(0)),
                revokingShards = listOf(RevokingShardReport(ShardId(1), RevokingShardState.REVOKED)),
            ),
        )
        clock.advance(Duration.ofSeconds(6))
        val assignedToB = service.heartbeat(
            "rebalance-completes",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch),
        )
        val members = service.listMembers("rebalance-completes", "orders-consumer").members

        assertEquals(setOf(ShardId(1)), assignedToB.assignment.assignedShards)
        assertEquals(MemberState.ACTIVE, members.single { it.memberId == "member-a" }.state)
    }

    @Test
    fun `failover resumes pending revoke`() {
        val sharedStore = InMemoryCoordinatorStateStore()
        val firstCoordinator = service(clock, sharedStore)
        firstCoordinator.createGroup("failover", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = firstCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        firstCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val memberB = firstCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        assertEquals(setOf(ShardId(1)), memberB.assignment.pendingShards)

        val replacementCoordinator = service(clock, sharedStore)
        val revokeAck = replacementCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberA.memberEpoch,
                ownedShards = setOf(ShardId(0)),
                revokingShards = listOf(RevokingShardReport(ShardId(1), RevokingShardState.REVOKED)),
            ),
        )
        val assignedToB = replacementCoordinator.heartbeat(
            "failover",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch),
        )

        assertEquals(setOf(ShardId(0)), revokeAck.assignment.assignedShards)
        assertEquals(setOf(ShardId(1)), assignedToB.assignment.assignedShards)
        assertTrue(replacementCoordinator.assignments("failover", "orders-consumer").invariantViolations.isEmpty())
    }

    @Test
    fun `heartbeat fences stale member epoch`() {
        service.createGroup("stale-epoch", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "stale-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "stale-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val stale = service.heartbeat(
            "stale-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = acknowledged.memberEpoch - 1, ownedShards = memberA.assignment.assignedShards),
        )

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, stale.status)
        assertEquals(acknowledged.memberEpoch, stale.memberEpoch)
        assertTrue(stale.assignment.assignedShards.isEmpty())
        assertTrue(stale.assignment.pendingShards.isEmpty())
    }

    @Test
    fun `heartbeat rejects active member epoch reset`() {
        service.createGroup("epoch-reset", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "epoch-reset",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "epoch-reset",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val resetAttempt = service.heartbeat(
            "epoch-reset",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0, ownedShards = memberA.assignment.assignedShards),
        )
        val members = service.listMembers("epoch-reset", "orders-consumer").members
        val assignments = service.assignments("epoch-reset", "orders-consumer")

        assertEquals(HeartbeatStatus.INVALID_REQUEST, resetAttempt.status)
        assertEquals(memberA.memberEpoch, members.single { it.memberId == "member-a" }.memberEpoch)
        assertEquals(setOf(ShardId(0), ShardId(1)), assignments.currentAssignments.getValue("member-a"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `heartbeat rejects epoch reset after ownership was acknowledged even when report is empty`() {
        service.createGroup("epoch-reset-empty", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val joined = service.heartbeat(
            "epoch-reset-empty",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "epoch-reset-empty",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = joined.memberEpoch, ownedShards = joined.assignment.assignedShards),
        )

        val resetAttempt = service.heartbeat(
            "epoch-reset-empty",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val member = service.listMembers("epoch-reset-empty", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.INVALID_REQUEST, resetAttempt.status)
        assertEquals(acknowledged.memberEpoch, member.memberEpoch)
        assertEquals(joined.assignment.assignedShards, member.currentAssignment)
    }

    @Test
    fun `heartbeat rejects client advanced member epoch`() {
        service.createGroup("future-epoch", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "future-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "future-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val future = service.heartbeat(
            "future-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = acknowledged.memberEpoch + 1, ownedShards = memberA.assignment.assignedShards),
        )
        val member = service.listMembers("future-epoch", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.INVALID_REQUEST, future.status)
        assertEquals(acknowledged.memberEpoch, member.memberEpoch)
        assertEquals(setOf(ShardId(0), ShardId(1)), member.currentAssignment)
    }

    @Test
    fun `heartbeat with higher metadata version requests sync to current redis metadata`() {
        service.createGroup("metadata-sync", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )

        val sync = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = acknowledged.memberEpoch + 10,
                metadataVersion = acknowledged.metadataVersion + 10,
                ownedShards = memberA.assignment.assignedShards,
            ),
        )
        val member = service.listMembers("metadata-sync", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.SYNC_METADATA, sync.status)
        assertEquals(acknowledged.metadataVersion, sync.metadataVersion)
        assertEquals(acknowledged.memberEpoch, sync.memberEpoch)
        assertEquals(memberA.assignment.assignedShards, sync.assignment.assignedShards)
        assertEquals(acknowledged.metadataVersion + 10, member.metadataVersion)
        assertEquals(memberA.assignment.assignedShards, member.currentAssignment)

        val retriedAfterLostResponse = service.heartbeat(
            "metadata-sync",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = acknowledged.memberEpoch + 10,
                metadataVersion = acknowledged.metadataVersion + 10,
                ownedShards = memberA.assignment.assignedShards,
            ),
        )

        assertEquals(HeartbeatStatus.SYNC_METADATA, retriedAfterLostResponse.status)
        assertEquals(acknowledged.metadataVersion, retriedAfterLostResponse.metadataVersion)
    }

    @Test
    fun `metadata sync ignores stale revoke reports from discarded higher version`() {
        val group = convergeTwoMemberGroup("metadata-sync-revoke")
        val current = service.getGroup("metadata-sync-revoke", "orders-consumer")
        val staleForeignShard = group.memberATarget.first()
        val staleOwnedByB = group.memberBTarget + staleForeignShard

        val sync = service.heartbeat(
            "metadata-sync-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = group.memberB.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = staleOwnedByB,
            ),
        )
        val correctedB = service.heartbeat(
            "metadata-sync-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = sync.memberEpoch,
                metadataVersion = sync.metadataVersion,
                ownedShards = group.memberBTarget,
                revokingShards = listOf(RevokingShardReport(staleForeignShard, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val correctedA = service.heartbeat(
            "metadata-sync-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = group.memberA.memberEpoch,
                metadataVersion = sync.metadataVersion,
                ownedShards = group.memberATarget,
            ),
        )
        val memberB = service.listMembers("metadata-sync-revoke", "orders-consumer").members.single { it.memberId == "member-b" }

        assertEquals(HeartbeatStatus.SYNC_METADATA, sync.status)
        assertEquals(HeartbeatStatus.REVOKE_PENDING, correctedB.status)
        assertEquals(HeartbeatStatus.OK, correctedA.status)
        assertEquals(group.memberBTarget, memberB.currentAssignment)
        assertTrue(memberB.revoking.isEmpty())
    }

    @Test
    fun `metadata sync does not expose new target shards as immediately assigned`() {
        val group = convergeTwoMemberGroup("metadata-sync-new-pending")
        val current = service.getGroup("metadata-sync-new-pending", "orders-consumer")

        val sync = service.heartbeat(
            "metadata-sync-new-pending",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = group.memberB.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = group.memberATarget,
            ),
        )

        assertEquals(HeartbeatStatus.SYNC_METADATA, sync.status)
        assertTrue(sync.assignment.assignedShards.isEmpty())
        assertEquals(group.memberBTarget, sync.assignment.pendingShards)
    }

    @Test
    fun `metadata sync waits in revoke pending until draining member releases and peers acknowledge`() {
        service.createGroup("metadata-sync-draining", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberAJoined = service.heartbeat(
            "metadata-sync-draining",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val memberAOwnedAll = service.heartbeat(
            "metadata-sync-draining",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberAJoined.memberEpoch,
                ownedShards = memberAJoined.assignment.assignedShards,
            ),
        )
        val memberBJoined = service.heartbeat(
            "metadata-sync-draining",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        val targetAssignments = service.assignments("metadata-sync-draining", "orders-consumer").targetAssignment
        val memberATarget = targetAssignments.getValue("member-a")
        val memberBTarget = targetAssignments.getValue("member-b")
        val releasedByA = memberAOwnedAll.assignment.assignedShards - memberATarget
        val current = service.getGroup("metadata-sync-draining", "orders-consumer")

        val syncA = service.heartbeat(
            "metadata-sync-draining",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberAOwnedAll.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = memberAOwnedAll.assignment.assignedShards,
            ),
        )
        val drainingA = service.heartbeat(
            "metadata-sync-draining",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = syncA.memberEpoch,
                metadataVersion = syncA.metadataVersion,
                ownedShards = memberATarget,
                revokingShards = releasedByA.map { RevokingShardReport(it, RevokingShardState.DRAINING, inFlight = 1) },
            ),
        )
        val revokedA = service.heartbeat(
            "metadata-sync-draining",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = drainingA.memberEpoch,
                metadataVersion = syncA.metadataVersion,
                ownedShards = memberATarget,
                revokingShards = releasedByA.map { RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0) },
            ),
        )
        val assignedB = service.heartbeat(
            "metadata-sync-draining",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = memberBJoined.memberEpoch,
                metadataVersion = syncA.metadataVersion,
                ownedShards = emptySet(),
            ),
        )

        assertEquals(HeartbeatStatus.SYNC_METADATA, syncA.status)
        assertEquals(memberATarget, syncA.assignment.assignedShards)
        assertEquals(HeartbeatStatus.REVOKE_PENDING, drainingA.status)
        assertEquals(HeartbeatStatus.REVOKE_PENDING, revokedA.status)
        assertEquals(HeartbeatStatus.OK, assignedB.status)
        assertEquals(memberBTarget, assignedB.assignment.assignedShards)
    }

    @Test
    fun `metadata sync does not wait for empty leaving members`() {
        service.createGroup("metadata-sync-empty-leaving", "orders-consumer", createGroupRequest(initialShardCount = 2))
        service.heartbeat(
            "metadata-sync-empty-leaving",
            "orders-consumer",
            "member-old",
            heartbeat("member-old", memberEpoch = 0),
        )
        service.heartbeat(
            "metadata-sync-empty-leaving",
            "orders-consumer",
            "member-old",
            heartbeat("member-old", memberEpoch = -1),
        )
        val memberAJoined = service.heartbeat(
            "metadata-sync-empty-leaving",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val current = service.getGroup("metadata-sync-empty-leaving", "orders-consumer")

        val syncA = service.heartbeat(
            "metadata-sync-empty-leaving",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberAJoined.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = emptySet(),
            ),
        )
        val correctedA = service.heartbeat(
            "metadata-sync-empty-leaving",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = syncA.memberEpoch,
                metadataVersion = syncA.metadataVersion,
                ownedShards = emptySet(),
            ),
        )
        service.heartbeat(
            "metadata-sync-empty-leaving",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = correctedA.memberEpoch,
                metadataVersion = correctedA.metadataVersion,
                ownedShards = correctedA.assignment.assignedShards,
            ),
        )
        val assignments = service.assignments("metadata-sync-empty-leaving", "orders-consumer")

        assertEquals(HeartbeatStatus.SYNC_METADATA, syncA.status)
        assertEquals(HeartbeatStatus.OK, correctedA.status)
        assertEquals(setOf(ShardId(0), ShardId(1)), correctedA.assignment.assignedShards)
        assertEquals(setOf(ShardId(0), ShardId(1)), assignments.currentAssignments.getValue("member-a"))
        assertEquals(emptySet(), assignments.currentAssignments.getValue("member-old"))
    }

    @Test
    fun `admin mutations are blocked while metadata sync is in progress`() {
        service.createGroup("metadata-sync-admin", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val joined = service.heartbeat(
            "metadata-sync-admin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val acknowledged = service.heartbeat(
            "metadata-sync-admin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = joined.memberEpoch, ownedShards = joined.assignment.assignedShards),
        )
        val current = service.getGroup("metadata-sync-admin", "orders-consumer")

        val sync = service.heartbeat(
            "metadata-sync-admin",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = acknowledged.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = joined.assignment.assignedShards,
            ),
        )
        val scaleError = kotlin.runCatching {
            service.scaleGroup(
                "metadata-sync-admin",
                "orders-consumer",
                ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "must wait for metadata sync"),
            )
        }.exceptionOrNull() as CoordinatorException

        assertEquals(HeartbeatStatus.SYNC_METADATA, sync.status)
        assertEquals(CoordinatorError.METADATA_SYNC_IN_PROGRESS, scaleError.error)

        service.createGroup("metadata-sync-rollback", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val rollbackJoined = service.heartbeat(
            "metadata-sync-rollback",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val rollbackOwned = service.heartbeat(
            "metadata-sync-rollback",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = rollbackJoined.memberEpoch,
                ownedShards = rollbackJoined.assignment.assignedShards,
            ),
        )
        val migration = service.scaleGroup(
            "metadata-sync-rollback",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "prepare rollback race"),
        )
        val afterScale = service.getGroup("metadata-sync-rollback", "orders-consumer")
        service.heartbeat(
            "metadata-sync-rollback",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = rollbackOwned.memberEpoch + 5,
                metadataVersion = afterScale.metadataVersion + 5,
                ownedShards = rollbackOwned.assignment.assignedShards,
            ),
        )

        val rollbackError = kotlin.runCatching {
            service.rollbackMigration("metadata-sync-rollback", "orders-consumer", migration.reshardingId)
        }.exceptionOrNull() as CoordinatorException

        assertEquals(CoordinatorError.METADATA_SYNC_IN_PROGRESS, rollbackError.error)
    }

    @Test
    fun `metadata sync serializes concurrent stale revoke reports from multiple members`() {
        val group = convergeTwoMemberGroup("metadata-sync-concurrent-revoke")
        val current = service.getGroup("metadata-sync-concurrent-revoke", "orders-consumer")
        val staleForeignShardForA = group.memberBTarget.first()
        val staleForeignShardForB = group.memberATarget.first()

        val syncA = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = group.memberA.memberEpoch + 5,
                metadataVersion = current.metadataVersion + 5,
                ownedShards = group.memberATarget + staleForeignShardForA,
            ),
        )
        val syncB = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = group.memberB.memberEpoch + 6,
                metadataVersion = current.metadataVersion + 6,
                ownedShards = group.memberBTarget + staleForeignShardForB,
            ),
        )

        val correctedA = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = syncA.memberEpoch,
                metadataVersion = syncA.metadataVersion,
                ownedShards = group.memberATarget,
                revokingShards = listOf(RevokingShardReport(staleForeignShardForA, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val correctedB = service.heartbeat(
            "metadata-sync-concurrent-revoke",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = syncB.memberEpoch,
                metadataVersion = syncB.metadataVersion,
                ownedShards = group.memberBTarget,
                revokingShards = listOf(RevokingShardReport(staleForeignShardForB, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val members = service.listMembers("metadata-sync-concurrent-revoke", "orders-consumer").members.associateBy { it.memberId }
        val finalGroup = service.getGroup("metadata-sync-concurrent-revoke", "orders-consumer")

        assertEquals(HeartbeatStatus.SYNC_METADATA, syncA.status)
        assertEquals(HeartbeatStatus.SYNC_METADATA, syncB.status)
        assertEquals(HeartbeatStatus.REVOKE_PENDING, correctedA.status)
        assertEquals(HeartbeatStatus.OK, correctedB.status)
        assertEquals(current.metadataVersion, finalGroup.metadataVersion)
        assertEquals(group.memberATarget, members.getValue("member-a").currentAssignment)
        assertEquals(group.memberBTarget, members.getValue("member-b").currentAssignment)
        assertTrue(members.getValue("member-a").revoking.isEmpty())
        assertTrue(members.getValue("member-b").revoking.isEmpty())
    }

    @Test
    fun `heartbeat rejects unsupported negative member epoch`() {
        service.createGroup("negative-epoch", "orders-consumer", createGroupRequest(initialShardCount = 2))

        val rejected = service.heartbeat(
            "negative-epoch",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = -2),
        )

        assertEquals(HeartbeatStatus.INVALID_REQUEST, rejected.status)
        assertTrue(service.listMembers("negative-epoch", "orders-consumer").members.isEmpty())
    }

    @Test
    fun `membership fences expired owner stale epoch`() {
        service.createGroup("expired-return", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        val memberB = service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )

        val staleMemberA = service.heartbeat(
            "expired-return",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        val assignments = service.assignments("expired-return", "orders-consumer")

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, staleMemberA.status)
        assertEquals(setOf(ShardId(0), ShardId(1)), memberB.assignment.assignedShards)
        assertEquals(setOf(ShardId(0), ShardId(1)), assignments.targetAssignment.getValue("member-b"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `monitoring current assignments ignore expired members`() {
        service.createGroup("monitor-expired-current", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "monitor-expired-current",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "monitor-expired-current",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        val memberB = service.heartbeat(
            "monitor-expired-current",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        service.heartbeat(
            "monitor-expired-current",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch, ownedShards = memberB.assignment.assignedShards),
        )

        val group = service.getGroup("monitor-expired-current", "orders-consumer")
        val grafana = service.grafanaGroups().single { it.streamPrefix == "monitor-expired-current" }

        assertEquals(mapOf("member-b" to 2), group.currentAssignmentSummary)
        assertEquals(2, grafana.currentShards)
    }

    @Test
    fun `expired member leave heartbeat stays fenced`() {
        service.createGroup("expired-leave", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "expired-leave",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-leave",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        service.tick()

        val leave = service.heartbeat(
            "expired-leave",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = -1, ownedShards = memberA.assignment.assignedShards),
        )
        val member = service.listMembers("expired-leave", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, leave.status)
        assertEquals(MemberState.EXPIRED, member.state)
        assertEquals(memberA.assignment.assignedShards, member.currentAssignment)
    }

    @Test
    fun `membership ignores stale ownership on rejoin`() {
        service.createGroup("expired-rejoin", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        clock.advance(Duration.ofSeconds(16))
        val memberB = service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch, ownedShards = memberB.assignment.assignedShards),
        )

        val rejoinedA = service.heartbeat(
            "expired-rejoin",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0, ownedShards = memberA.assignment.assignedShards),
        )
        val assignments = service.assignments("expired-rejoin", "orders-consumer")

        assertEquals(HeartbeatStatus.OK, rejoinedA.status)
        assertTrue(rejoinedA.assignment.assignedShards.isEmpty())
        assertEquals(emptySet(), assignments.currentAssignments.getValue("member-a"))
        assertEquals(setOf(ShardId(0), ShardId(1)), assignments.currentAssignments.getValue("member-b"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `heartbeat fences member that reports pending shard ownership before revoke completes`() {
        service.createGroup("premature-ownership", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = memberA.memberEpoch, ownedShards = memberA.assignment.assignedShards),
        )
        val memberB = service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        val pendingShard = memberB.assignment.pendingShards.single()

        val fenced = service.heartbeat(
            "premature-ownership",
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberB.memberEpoch, ownedShards = setOf(pendingShard)),
        )
        val members = service.listMembers("premature-ownership", "orders-consumer").members
        val assignments = service.assignments("premature-ownership", "orders-consumer")

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, fenced.status)
        assertTrue(fenced.assignment.assignedShards.isEmpty())
        assertEquals(MemberState.FENCED, members.single { it.memberId == "member-b" }.state)
        assertEquals(emptySet(), assignments.currentAssignments.getValue("member-b"))
        assertEquals(setOf(ShardId(0), ShardId(1)), assignments.targetAssignment.getValue("member-a"))
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `heartbeat fences member that reports shard owned by another active member`() {
        val converged = convergeTwoMemberGroup("foreign-ownership")
        val foreignShard = converged.memberATarget.single()

        val fenced = service.heartbeat(
            "foreign-ownership",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = converged.memberB.memberEpoch,
                ownedShards = converged.memberBTarget + foreignShard,
            ),
        )
        val members = service.listMembers("foreign-ownership", "orders-consumer").members
        val assignments = service.assignments("foreign-ownership", "orders-consumer")

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, fenced.status)
        assertEquals(MemberState.FENCED, members.single { it.memberId == "member-b" }.state)
        assertEquals(emptySet(), assignments.currentAssignments.getValue("member-b"))
        assertEquals(emptySet(), assignments.revokeProgress["member-b"].orEmpty())
        assertTrue(assignments.invariantViolations.isEmpty())
    }

    @Test
    fun `higher metadata version does not bypass fenced member state`() {
        val converged = convergeTwoMemberGroup("metadata-sync-fenced")
        val foreignShard = converged.memberATarget.single()
        service.heartbeat(
            "metadata-sync-fenced",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = converged.memberB.memberEpoch,
                ownedShards = converged.memberBTarget + foreignShard,
            ),
        )

        val retried = service.heartbeat(
            "metadata-sync-fenced",
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = converged.memberB.memberEpoch,
                metadataVersion = converged.memberB.metadataVersion + 10,
                ownedShards = converged.memberBTarget + foreignShard,
            ),
        )

        assertEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, retried.status)
        assertTrue(retried.assignment.assignedShards.isEmpty())
    }

    @Test
    fun `duplicate terminal revoke report after release does not fence member`() {
        val converged = convergeTwoMemberGroup("duplicate-revoke")
        val releasedByA = converged.memberBTarget.single()

        val response = service.heartbeat(
            "duplicate-revoke",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = converged.memberA.memberEpoch,
                ownedShards = converged.memberATarget,
                revokingShards = listOf(RevokingShardReport(releasedByA, RevokingShardState.REVOKED, inFlight = 0)),
            ),
        )
        val memberA = service.listMembers("duplicate-revoke", "orders-consumer").members.single { it.memberId == "member-a" }

        assertEquals(HeartbeatStatus.OK, response.status)
        assertEquals(MemberState.ACTIVE, memberA.state)
        assertEquals(converged.memberATarget, memberA.currentAssignment)
    }

    @Test
    fun `migration scale updates shard count`() {
        service.createGroup("summary", "summary-consumer", createGroupRequest(initialShardCount = 2))

        val migration = service.scaleGroup(
            "summary",
            "summary-consumer",
            ScaleGroupRequest(
                targetShardCount = 3,
                requestedBy = "test",
                reason = "scale out",
            ),
        )
        val group = service.getGroup("summary", "summary-consumer")

                        assertEquals(3, group.shardCount)
            }

    @Test
    fun `stream scale updates every consumer group and assignments arrive on heartbeat`() {
        service.createGroup("stream-scale", "orders-consumer", createGroupRequest(initialShardCount = 2))
        service.createGroup("stream-scale", "analytics-consumer", createGroupRequest(initialShardCount = 2))

        val ordersJoin = service.heartbeat(
            "stream-scale",
            "orders-consumer",
            "orders-member",
            heartbeat("orders-member", memberEpoch = 0),
        )
        val analyticsJoin = service.heartbeat(
            "stream-scale",
            "analytics-consumer",
            "analytics-member",
            heartbeat("analytics-member", memberEpoch = 0),
        )

        val response = service.scaleStream(
            "stream-scale",
            ScaleStreamRequest(targetShardCount = 4, requestedBy = "test", reason = "stream level scale"),
        )

        assertEquals("stream-scale", response.streamPrefix)
        assertEquals(4, response.targetShardCount)
        assertEquals(listOf("analytics-consumer", "orders-consumer"), response.affectedConsumerGroups)
        assertEquals(2, response.migrations.size)
        assertEquals(4, service.getGroup("stream-scale", "orders-consumer").shardCount)
        assertEquals(4, service.getGroup("stream-scale", "analytics-consumer").shardCount)

        val ordersHeartbeat = service.heartbeat(
            "stream-scale",
            "orders-consumer",
            "orders-member",
            heartbeat(
                "orders-member",
                memberEpoch = ordersJoin.memberEpoch,
                ownedShards = ordersJoin.assignment.assignedShards,
            ),
        )
        val analyticsHeartbeat = service.heartbeat(
            "stream-scale",
            "analytics-consumer",
            "analytics-member",
            heartbeat(
                "analytics-member",
                memberEpoch = analyticsJoin.memberEpoch,
                ownedShards = analyticsJoin.assignment.assignedShards,
            ),
        )

        assertEquals(setOf(ShardId(0), ShardId(1), ShardId(2), ShardId(3)), ordersHeartbeat.assignment.assignedShards)
        assertEquals(setOf(ShardId(0), ShardId(1), ShardId(2), ShardId(3)), analyticsHeartbeat.assignment.assignedShards)
    }

    @Test
    fun `scale in migration completes after removed shards are drained`() {
        service.createGroup("drain", "orders-consumer", createGroupRequest(initialShardCount = 3))
        val joined = service.heartbeat("drain", "orders-consumer", "member-a", heartbeat("member-a", memberEpoch = 0))
        val oldOwned = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = joined.memberEpoch, ownedShards = joined.assignment.assignedShards),
        )
        val migration = service.scaleGroup(
            "drain",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 1, requestedBy = "test", reason = "scale in"),
        )
        val oldAndNew = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = oldOwned.memberEpoch, ownedShards = oldOwned.assignment.assignedShards),
        )

        val drainingResponse = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = oldAndNew.memberEpoch, ownedShards = oldAndNew.assignment.assignedShards),
        )
        val drainingGroup = service.getGroup("drain", "orders-consumer")
        val drainingMigration = service.getMigration("drain", "orders-consumer", migration.reshardingId)

        assertEquals(MigrationState.DRAINING, drainingMigration.state)
        assertEquals(setOf(ShardId(0)), drainingResponse.assignment.assignedShards)
        assertEquals(setOf(ShardId(0)), service.assignments("drain", "orders-consumer").targetAssignment.getValue("member-a"))

        val finalized = service.heartbeat(
            "drain",
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = drainingResponse.memberEpoch,
                ownedShards = drainingResponse.assignment.assignedShards,
            ),
        )
        assertEquals(HeartbeatStatus.OK, finalized.status)
        val completedGroup = service.getGroup("drain", "orders-consumer")
        val completedMigration = service.getMigration("drain", "orders-consumer", migration.reshardingId)

        assertEquals(MigrationState.DEPRECATED, completedMigration.state)
        assertEquals(null, completedGroup.activeMigration)
        assertEquals(setOf(ShardId(0)), finalized.assignment.assignedShards)
    }

    @Test
    fun `migration scale lets live consumers revoke old assignments without fencing`() {
        service.createGroup("live-scale", "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberA = SimulatedConsumer("member-a")
        val memberB = SimulatedConsumer("member-b")

        fun poll(member: SimulatedConsumer): HeartbeatResponse {
            val response = service.heartbeat(
                "live-scale",
                "orders-consumer",
                member.memberId,
                heartbeat(
                    member.memberId,
                    memberEpoch = member.memberEpoch,
                    ownedShards = member.ownedShards,
                    revokingShards = member.revokingShards,
                ),
            )
            member.apply(response)
            return response
        }

        poll(memberA)
        poll(memberA)
        poll(memberB)
        poll(memberA)
        poll(memberB)
        poll(memberB)

        service.scaleGroup(
            "live-scale",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "live scale"),
        )

        repeat(8) {
            assertNotEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, poll(memberA).status)
            assertNotEquals(HeartbeatStatus.FENCED_MEMBER_EPOCH, poll(memberB).status)
        }
        val assignments = service.assignments("live-scale", "orders-consumer")
        val members = service.listMembers("live-scale", "orders-consumer").members

        assertTrue(assignments.invariantViolations.isEmpty())
        assertEquals(assignments.targetAssignment, assignments.currentAssignments.filterValues { it.isNotEmpty() })
        assertTrue(members.all { it.state == MemberState.ACTIVE })
            }

    @Test
    fun `producer routing returns shard routing metadata`() {
        service.createGroup(
            "route-orders",
            "orders-consumer",
            CreateGroupRequest(
                initialShardCount = 2,
                requestedBy = "test",
            ),
        )
        val beforeScale = service.producerRouting("route-orders", "orders-consumer")

        service.scaleGroup(
            "route-orders",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "scale producer writes"),
        )
        val afterScale = service.producerRouting("route-orders", "orders-consumer")

                assertEquals(2, beforeScale.shardCount)
        assertEquals("route-orders:{shardIndex}", beforeScale.streamKeyPattern)
        assertEquals(listOf("route-orders:0", "route-orders:1"), beforeScale.shards.map { it.streamKey })
                assertEquals(3, afterScale.shardCount)
        assertTrue(afterScale.metadataVersion > beforeScale.metadataVersion)
        assertEquals(
            listOf("route-orders:0", "route-orders:1", "route-orders:2"),
            afterScale.shards.map { it.streamKey },
        )
    }

    @Test
    fun `group creation provisions initial shard plan`() {
        val provisioner = RecordingStreamShardProvisioner()
        val service = service(clock, InMemoryCoordinatorStateStore(), provisioner)

        service.createGroup("provision-create", "orders-consumer", createGroupRequest(initialShardCount = 2))

        assertEquals(
            listOf(
                ProvisionedPlan("provision-create", "orders-consumer", shardCount = 2),
            ),
            provisioner.provisioned,
        )
    }

    @Test
    fun `scale provisions target shard plan after preparing migration state is committed`() {
        val provisioner = RecordingStreamShardProvisioner()
        val service = service(clock, InMemoryCoordinatorStateStore(), provisioner)

        service.createGroup("provision-scale", "orders-consumer", createGroupRequest(initialShardCount = 2))
        service.scaleGroup(
            "provision-scale",
            "orders-consumer",
            ScaleGroupRequest(targetShardCount = 3, requestedBy = "test", reason = "provision target shard count"),
        )

        assertEquals(
            listOf(
                ProvisionedPlan("provision-scale", "orders-consumer", shardCount = 2),
                ProvisionedPlan("provision-scale", "orders-consumer", shardCount = 3),
            ),
            provisioner.provisioned,
        )
    }

    private fun service(clock: Clock): CoordinatorService =
        service(clock, InMemoryCoordinatorStateStore())

    private fun service(
        clock: Clock,
        stateStore: CoordinatorStateStore = InMemoryCoordinatorStateStore(),
        streamProvisioner: StreamShardProvisioner = NoopStreamShardProvisioner,
        rebalanceTimeout: Duration = CoordinatorProtocol.DEFAULT_TIMING.rebalanceTimeout,
        properties: CoordinatorProperties = CoordinatorProperties(
            heartbeatInterval = Duration.ofSeconds(3),
            memberLeaseTtl = Duration.ofSeconds(15),
            rebalanceTimeout = rebalanceTimeout,
            defaults = CoordinatorProperties.Defaults(
                initialShardCount = 4,
                consumerMaxConcurrency = 4,
            ),
        ),
        redisCommands: CoordinatorRedisCommands = CoordinatorRedisCommands(
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java).ifAvailable,
        ),
    ): CoordinatorService =
        CoordinatorService(
            properties = properties,
            stateStore = stateStore,
            redisConnectionFactory = StaticListableBeanFactory().getBeanProvider(RedisConnectionFactory::class.java),
            streamProvisioner = streamProvisioner,
            clock = clock,
            redisCommands = redisCommands,
        )

    private data class ConvergedTwoMemberGroup(
        val memberA: HeartbeatResponse,
        val memberB: HeartbeatResponse,
        val memberATarget: Set<ShardId>,
        val memberBTarget: Set<ShardId>,
    )

    private data class SimulatedConsumer(
        val memberId: String,
        var memberEpoch: Long = 0,
        var ownedShards: Set<ShardId> = emptySet(),
        var revokingShards: List<RevokingShardReport> = emptyList(),
    ) {
        fun apply(response: HeartbeatResponse) {
            when (response.status) {
                HeartbeatStatus.OK -> {
                    memberEpoch = response.memberEpoch
                    val nextOwned = response.assignment.assignedShards
                    val revoked = ownedShards - nextOwned
                    ownedShards = nextOwned
                    revokingShards = revoked.map {
                        RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                    }
                }
                HeartbeatStatus.SYNC_METADATA, HeartbeatStatus.REVOKE_PENDING -> {
                    memberEpoch = response.memberEpoch
                    val nextOwned = ownedShards.intersect(response.assignment.assignedShards)
                    val revoked = ownedShards - nextOwned
                    ownedShards = nextOwned
                    revokingShards = revoked.map {
                        RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                    }
                }
                HeartbeatStatus.FENCED_MEMBER_EPOCH, HeartbeatStatus.UNKNOWN_MEMBER_ID -> {
                    memberEpoch = 0
                    ownedShards = emptySet()
                    revokingShards = emptyList()
                }
                HeartbeatStatus.RETRY -> Unit
                HeartbeatStatus.UNSUPPORTED_PROTOCOL, HeartbeatStatus.INVALID_REQUEST ->
                    error("Unexpected heartbeat status ${response.status}")
            }
        }
    }

    private fun convergeTwoMemberGroup(streamPrefix: String): ConvergedTwoMemberGroup {
        service.createGroup(streamPrefix, "orders-consumer", createGroupRequest(initialShardCount = 2))
        val memberAJoined = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-a",
            heartbeat("member-a", memberEpoch = 0),
        )
        val memberAOwnedAll = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberAJoined.memberEpoch,
                ownedShards = memberAJoined.assignment.assignedShards,
            ),
        )
        val memberBJoined = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = 0),
        )
        val targetAssignments = service.assignments(streamPrefix, "orders-consumer").targetAssignment
        val memberATarget = targetAssignments.getValue("member-a")
        val memberBTarget = targetAssignments.getValue("member-b")
        val releasedByA = memberAOwnedAll.assignment.assignedShards - memberATarget
        val memberAReleased = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-a",
            heartbeat(
                "member-a",
                memberEpoch = memberAOwnedAll.memberEpoch,
                ownedShards = memberATarget,
                revokingShards = releasedByA.map {
                    RevokingShardReport(it, RevokingShardState.REVOKED, inFlight = 0)
                },
            ),
        )
        val memberBAssigned = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-b",
            heartbeat("member-b", memberEpoch = memberBJoined.memberEpoch),
        )
        val memberBOwnedTarget = service.heartbeat(
            streamPrefix,
            "orders-consumer",
            "member-b",
            heartbeat(
                "member-b",
                memberEpoch = memberBAssigned.memberEpoch,
                ownedShards = memberBAssigned.assignment.assignedShards,
            ),
        )

        return ConvergedTwoMemberGroup(
            memberA = memberAReleased,
            memberB = memberBOwnedTarget,
            memberATarget = memberATarget,
            memberBTarget = memberBTarget,
        )
    }

    private fun redisProvider(redisConnectionFactory: RedisConnectionFactory) =
        StaticListableBeanFactory()
            .apply { addBean("redisConnectionFactory", redisConnectionFactory) }
            .getBeanProvider(RedisConnectionFactory::class.java)

    private fun createGroupRequest(initialShardCount: Int? = null): CreateGroupRequest =
        CreateGroupRequest(
            initialShardCount = initialShardCount,
            requestedBy = "test",
        )

    private fun heartbeat(
        memberId: String,
        memberEpoch: Long,
        ownedShards: Set<ShardId> = emptySet(),
        revokingShards: List<RevokingShardReport> = emptyList(),
        protocolVersion: Int = 1,
        metadataVersion: Long = 0,
    ): HeartbeatRequest =
        HeartbeatRequest(
            protocolVersion = protocolVersion,
            requestId = "hb-$memberId-$memberEpoch",
            memberId = memberId,
            memberName = memberId,
            memberEpoch = memberEpoch,
            metadataVersion = metadataVersion,
            runtimeConsumerCapacity = RuntimeConsumerCapacity(
                runtimeMaxConcurrency = 4,
                availableConcurrency = 4,
            ),
            ownedShards = ownedShards,
            revokingShards = revokingShards,
        )
}

private data class ProvisionedPlan(
    val streamPrefix: String,
    val consumerGroup: String,
    val shardCount: Int,
)

private class RecordingStreamShardProvisioner : StreamShardProvisioner {
    val provisioned = mutableListOf<ProvisionedPlan>()

    override fun provision(plan: RedisStreamShardProvisioningPlan) {
        provisioned += ProvisionedPlan(
            streamPrefix = plan.streamPrefix,
            consumerGroup = plan.consumerGroup,
            shardCount = plan.shardCount,
        )
    }
}

private class FakeMessageRedisCommands(
    private val recordsByStreamKey: Map<String, List<RedisStreamRecord>>,
) : CoordinatorRedisCommands() {
    val rangeRequests = mutableListOf<String>()

    override fun xInfoStream(streamKey: String): RedisStreamInfo =
        RedisStreamInfo(
            length = recordsByStreamKey[streamKey]?.size?.toLong() ?: 0,
            firstEntryId = recordsByStreamKey[streamKey]?.firstOrNull()?.id,
            lastEntryId = recordsByStreamKey[streamKey]?.lastOrNull()?.id,
            lastGeneratedId = recordsByStreamKey[streamKey]?.lastOrNull()?.id,
            entriesAdded = recordsByStreamKey[streamKey]?.size?.toLong() ?: 0,
        )

    override fun xRange(streamKey: String, start: String, end: String, count: Long): List<RedisStreamRecord> {
        rangeRequests += streamKey
        return recordsByStreamKey[streamKey]
            .orEmpty()
            .filter { record -> afterOrEqual(record.id, start) && beforeOrEqual(record.id, end) }
            .take(count.toInt())
    }

    override fun xRevRange(streamKey: String, start: String, end: String, count: Long): List<RedisStreamRecord> {
        rangeRequests += streamKey
        return recordsByStreamKey[streamKey]
            .orEmpty()
            .asReversed()
            .filter { record -> beforeOrEqual(record.id, start) && afterOrEqual(record.id, end) }
            .take(count.toInt())
    }

    private fun afterOrEqual(recordId: String, bound: String): Boolean =
        bound == "-" || compareRecordIds(recordId, bound) >= 0

    private fun beforeOrEqual(recordId: String, bound: String): Boolean =
        bound == "+" || compareRecordIds(recordId, bound) <= 0

    private fun compareRecordIds(left: String, right: String): Int {
        val leftParts = left.split("-", limit = 2)
        val rightParts = right.split("-", limit = 2)
        val msCompare = leftParts.getOrNull(0).orEmpty().toLong()
            .compareTo(rightParts.getOrNull(0).orEmpty().toLong())
        if (msCompare != 0) {
            return msCompare
        }
        return leftParts.getOrNull(1).orEmpty().toLong()
            .compareTo(rightParts.getOrNull(1).orEmpty().toLong())
    }
}

private class FakeExistingKeyRedisCommands(
    existingKeys: Set<String>,
) : CoordinatorRedisCommands() {
    val existingKeys: MutableSet<String> = existingKeys.toMutableSet()

    override fun isConfigured(): Boolean =
        true

    override fun hasKey(key: String): Boolean =
        key in existingKeys
}

private class FakeOffsetRedisCommands(
    private val delayMs: Long = 0,
) : CoordinatorRedisCommands() {
    private val shards = mutableMapOf<String, OffsetState>()
    val xInfoStreamCalls = AtomicInteger(0)
    val maxConcurrentReads = AtomicInteger(0)
    private val activeReads = AtomicInteger(0)

    fun setShard(streamKey: String, length: Long, lag: Long?) {
        shards[streamKey] = OffsetState(length = length, lag = lag)
    }

    override fun xInfoStream(streamKey: String): RedisStreamInfo {
        xInfoStreamCalls.incrementAndGet()
        return trackConcurrentRead {
            val state = shards[streamKey] ?: OffsetState(length = 0, lag = null)
            RedisStreamInfo(
                length = state.length,
                firstEntryId = if (state.length > 0) "1-0" else null,
                lastEntryId = if (state.length > 0) "${state.length}-0" else null,
                lastGeneratedId = if (state.length > 0) "${state.length}-0" else null,
                entriesAdded = state.length,
            )
        }
    }

    override fun xInfoGroups(streamKey: String): List<RedisStreamGroupInfo> {
        val state = shards[streamKey] ?: return emptyList()
        return listOf(
            RedisStreamGroupInfo(
                name = "orders-consumer",
                consumers = 1,
                pending = 0,
                lastDeliveredId = if (state.length > 0) "${state.length}-0" else null,
                entriesRead = state.lag?.let { state.length - it },
                lag = state.lag,
            ),
        )
    }

    override fun memoryUsage(key: String): Long? =
        shards[key]?.length?.times(32)

    private fun <T> trackConcurrentRead(block: () -> T): T {
        val active = activeReads.incrementAndGet()
        maxConcurrentReads.updateAndGet { current -> maxOf(current, active) }
        try {
            if (delayMs > 0) {
                Thread.sleep(delayMs)
            }
            return block()
        } finally {
            activeReads.decrementAndGet()
        }
    }

    private data class OffsetState(
        val length: Long,
        val lag: Long?,
    )
}

private class CopyingConflictOnceStateStore : CoordinatorStateStore {
    private val groups = linkedMapOf<GroupKey, GroupMetadata>()
    var conflictsBeforeSave: Int = 0
    var conflictedSaves: Int = 0
        private set

    override fun contains(key: GroupKey): Boolean =
        key in groups

    override fun get(key: GroupKey): GroupMetadata? =
        groups[key]?.deepCopy()

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean {
        if (key in groups) {
            return false
        }
        val stored = group.deepCopy()
        stored.storeRevision = 1
        group.storeRevision = stored.storeRevision
        groups[key] = stored
        return true
    }

    override fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean {
        if (groups[key]?.storeRevision != expectedRevision) {
            return false
        }
        groups.remove(key)
        return true
    }

    override fun save(key: GroupKey, group: GroupMetadata) {
        if (conflictsBeforeSave > 0) {
            conflictsBeforeSave -= 1
            conflictedSaves += 1
            throw CoordinatorStateConflictException("injected monitoring save conflict")
        }
        val stored = group.deepCopy()
        stored.storeRevision = group.storeRevision + 1
        group.storeRevision = stored.storeRevision
        groups[key] = stored
    }

    override fun list(): List<GroupMetadata> =
        groups.values.map { it.deepCopy() }

    private fun GroupMetadata.deepCopy(): GroupMetadata =
        copy(
            members = members
                .mapValues { (_, member) ->
                    member.copy(
                        currentAssignment = member.currentAssignment.toSet(),
                        revoking = member.revoking.toSet(),
                    )
                }
                .toMutableMap(),
            targetAssignments = targetAssignments
                .mapValues { (_, shards) -> shards.toMutableSet() }
                .toMutableMap(),
            migrations = migrations.mapValues { (_, migration) -> migration.copy() }.toMutableMap(),
        )
}

private fun waitUntil(timeoutMs: Long = 1_000, condition: () -> Boolean) {
    val deadline = System.nanoTime() + timeoutMs * 1_000_000
    while (System.nanoTime() < deadline) {
        if (condition()) {
            return
        }
        Thread.sleep(10)
    }
    assertTrue(condition(), "condition was not met within ${timeoutMs}ms")
}

private class MutableClock(
    private var current: Instant,
    private val zone: ZoneId,
) : Clock() {
    override fun getZone(): ZoneId = zone

    override fun withZone(zone: ZoneId): Clock =
        MutableClock(current, zone)

    override fun instant(): Instant = current

    fun advance(duration: Duration) {
        current = current.plus(duration)
    }
}
