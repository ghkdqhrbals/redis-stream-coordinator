package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.COORDINATOR_METADATA_SCHEMA_VERSION
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ConsumerConcurrencyPolicy
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.Migration
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MigrationState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ShardId
import io.github.ghkdqhrbals.redisstreamcoordinator.store.CoordinatorStateSchemaException
import io.github.ghkdqhrbals.redisstreamcoordinator.store.readRedisGroupMetadata
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import tools.jackson.databind.ObjectMapper
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

@SpringBootTest(
    properties = [
        "coordinator.store.type=memory",
    ],
)
class RedisCoordinatorMetadataSchemaTest {
    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Test
    fun `redis metadata parser accepts current and legacy v1 metadata`() {
        val raw = objectMapper.writeValueAsString(groupMetadata())
        val legacyRaw = raw.replace("\"schemaVersion\":$COORDINATOR_METADATA_SCHEMA_VERSION,", "")

        assertEquals(COORDINATOR_METADATA_SCHEMA_VERSION, objectMapper.readRedisGroupMetadata(raw).schemaVersion)
        assertEquals(COORDINATOR_METADATA_SCHEMA_VERSION, objectMapper.readRedisGroupMetadata(legacyRaw).schemaVersion)
    }

    @Test
    fun `redis metadata parser ignores removed producer routing fields`() {
        val raw = objectMapper.writeValueAsString(groupMetadata())
        val legacyRaw = raw.replace(
            "\"consumerConcurrencyPolicy\":",
            "\"hashAlgorithm\":\"murmur3\",\"hashSeed\":\"default\",\"consumerConcurrencyPolicy\":",
        )

        assertEquals(COORDINATOR_METADATA_SCHEMA_VERSION, objectMapper.readRedisGroupMetadata(legacyRaw).schemaVersion)
    }

    @Test
    fun `redis metadata parser accepts legacy migration id fields`() {
        val raw = objectMapper.writeValueAsString(
            groupMetadata().also { group ->
                val migration = Migration(
                    reshardingId = "reshard-1",
                    fromShardCount = 4,
                    toShardCount = 8,
                    state = MigrationState.ACTIVE,
                    createdAt = Instant.parse("2026-05-24T00:00:01Z"),
                    updatedAt = Instant.parse("2026-05-24T00:00:02Z"),
                )
                group.migrations[migration.reshardingId] = migration
                group.activeReshardingId = migration.reshardingId
            },
        )
        val legacyRaw = raw
            .replace("\"reshardingId\":\"reshard-1\"", "\"migrationId\":\"reshard-1\"")
            .replace("\"activeReshardingId\":\"reshard-1\"", "\"activeMigrationId\":\"reshard-1\"")

        val parsed = objectMapper.readRedisGroupMetadata(legacyRaw)

        assertEquals("reshard-1", parsed.activeReshardingId)
        assertEquals("reshard-1", parsed.migrations.getValue("reshard-1").reshardingId)
    }

    @Test
    fun `redis metadata parser upgrades legacy coordinator managed shard layout metadata`() {
        val legacyRaw = legacyManagedLayoutMetadata()

        val parsed = objectMapper.readRedisGroupMetadata(legacyRaw)

        assertEquals(4, parsed.shardCount)
        assertEquals(setOf(ShardId(1)), parsed.members.getValue("member-a").currentAssignment)
        assertEquals(setOf(ShardId(0)), parsed.members.getValue("member-a").revoking)
        assertEquals(2, parsed.migrations.getValue("reshard-legacy").fromShardCount)
        assertEquals(4, parsed.migrations.getValue("reshard-legacy").toShardCount)
    }

    @Test
    fun `redis metadata parser rejects unsupported future schema`() {
        val raw = objectMapper.writeValueAsString(
            groupMetadata().also { it.schemaVersion = COORDINATOR_METADATA_SCHEMA_VERSION + 1 },
        )

        assertFailsWith<CoordinatorStateSchemaException> {
            objectMapper.readRedisGroupMetadata(raw)
        }
    }

    private fun groupMetadata(): GroupMetadata =
        GroupMetadata(
            streamPrefix = "schema-orders",
            consumerGroup = "orders-consumer",
            groupEpoch = 1,
            metadataVersion = 1,
            assignmentEpoch = 0,
            state = GroupState.EMPTY,
            shardCount = 4,
            consumerConcurrencyPolicy = ConsumerConcurrencyPolicy(defaultMaxConcurrency = 4),
            createdAt = Instant.parse("2026-05-24T00:00:00Z"),
            updatedAt = Instant.parse("2026-05-24T00:00:00Z"),
        )

    private fun legacyManagedLayoutMetadata(): String {
        val layoutField = "stream" + "Version"
        val activeField = "activeWrite" + "Version"
        val readableField = "readable" + "Versions"
        val countsField = "shardCountsBy" + "Version"
        val fromField = "from" + "Version"
        val toField = "to" + "Version"
        val legacyStreamKey = "schema-orders:" + "v2" + ":1"
        return """
            {
              "streamPrefix": "schema-orders",
              "consumerGroup": "orders-consumer",
              "schemaVersion": 1,
              "storeRevision": 7,
              "groupEpoch": 3,
              "metadataVersion": 5,
              "assignmentEpoch": 2,
              "state": "STABLE",
              "$activeField": "v2",
              "$readableField": ["v1", "v2"],
              "$countsField": {
                "v1": 2,
                "v2": 4
              },
              "consumerConcurrencyPolicy": {
                "defaultMaxConcurrency": 4,
                "memberOverrides": {}
              },
              "members": {
                "member-a": {
                  "memberId": "member-a",
                  "memberName": "worker-a",
                  "state": "ACTIVE",
                  "memberEpoch": 3,
                  "metadataVersion": 5,
                  "assignedMaxConcurrency": 4,
                  "runtimeMaxConcurrency": 4,
                  "activeConsumerWorkers": 1,
                  "currentAssignment": [
                    {
                      "$layoutField": "v2",
                      "shardIndex": 1
                    }
                  ],
                  "revoking": [
                    {
                      "$layoutField": "v1",
                      "shardIndex": 0
                    }
                  ],
                  "lastHeartbeatAt": "2026-05-24T00:00:03Z",
                  "memberLeaseExpiresAt": "2026-05-24T00:00:18Z",
                  "rebalanceTimeoutMs": 60000,
                  "rebalanceDeadlineAt": null,
                  "shardProgress": [
                    {
                      "shard": {
                        "$layoutField": "v2",
                        "shardIndex": 1
                      },
                      "streamKey": "$legacyStreamKey",
                      "lastDeliveredId": "1-0",
                      "lastAckedId": "1-0",
                      "pendingCount": 0,
                      "updatedAt": "2026-05-24T00:00:03Z"
                    }
                  ]
                }
              },
              "targetAssignments": {
                "member-a": [
                  {
                    "$layoutField": "v2",
                    "shardIndex": 1
                  }
                ]
              },
              "migrations": {
                "reshard-legacy": {
                  "migrationId": "reshard-legacy",
                  "$fromField": "v1",
                  "$toField": "v2",
                  "state": "ACTIVE",
                  "createdAt": "2026-05-24T00:00:01Z",
                  "updatedAt": "2026-05-24T00:00:02Z"
                }
              },
              "activeMigrationId": "reshard-legacy",
              "metadataCorrection": null,
              "createdAt": "2026-05-24T00:00:00Z",
              "updatedAt": "2026-05-24T00:00:03Z"
            }
        """.trimIndent()
    }
}
