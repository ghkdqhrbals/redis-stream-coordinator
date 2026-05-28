package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.COORDINATOR_METADATA_SCHEMA_VERSION
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ConsumerConcurrencyPolicy
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupState
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.Migration
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.MigrationState
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
                    fromVersion = 1,
                    toVersion = 2,
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
            activeWriteVersion = 1,
            readableVersions = setOf(1),
            shardCountsByVersion = linkedMapOf(1 to 4),
            consumerConcurrencyPolicy = ConsumerConcurrencyPolicy(defaultMaxConcurrency = 4),
            createdAt = Instant.parse("2026-05-24T00:00:00Z"),
            updatedAt = Instant.parse("2026-05-24T00:00:00Z"),
        )
}
