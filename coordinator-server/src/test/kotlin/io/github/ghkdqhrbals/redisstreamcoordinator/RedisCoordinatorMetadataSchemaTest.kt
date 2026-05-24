package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.domain.COORDINATOR_METADATA_SCHEMA_VERSION
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.ConsumerConcurrencyPolicy
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupState
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
            hashAlgorithm = "murmur3",
            hashSeed = "default",
            consumerConcurrencyPolicy = ConsumerConcurrencyPolicy(defaultMaxConcurrency = 4),
            createdAt = Instant.parse("2026-05-24T00:00:00Z"),
            updatedAt = Instant.parse("2026-05-24T00:00:00Z"),
        )
}
