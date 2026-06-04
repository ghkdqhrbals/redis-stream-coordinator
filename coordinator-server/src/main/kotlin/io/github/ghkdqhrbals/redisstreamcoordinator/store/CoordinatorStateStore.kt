package io.github.ghkdqhrbals.redisstreamcoordinator.store

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.COORDINATOR_METADATA_SCHEMA_VERSION
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupKey
import io.github.ghkdqhrbals.redisstreamcoordinator.domain.GroupMetadata
import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Component
import tools.jackson.databind.JsonNode
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.node.ObjectNode
import tools.jackson.module.kotlin.readValue
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap

interface CoordinatorStateStore {
    /**
     * Checks whether metadata for the group key exists.
     */
    fun contains(key: GroupKey): Boolean

    /**
     * Loads the aggregate coordinator metadata for a group.
     */
    fun get(key: GroupKey): GroupMetadata?

    /**
     * Creates a group only when no metadata already exists for the key.
     */
    fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean

    /**
     * Deletes all group-scoped state only when the stored revision matches the expected revision.
     */
    fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean

    /**
     * Persists a changed group and detects stale writers through the store revision.
     */
    fun save(key: GroupKey, group: GroupMetadata)

    /**
     * Lists all known group aggregates from the store index.
     */
    fun list(): List<GroupMetadata>
}

class CoordinatorStateConflictException(message: String) : RuntimeException(message)
class CoordinatorStateSchemaException(message: String) : RuntimeException(message)

@Component
@ConditionalOnProperty(prefix = "coordinator.store", name = ["type"], havingValue = "memory", matchIfMissing = true)
class InMemoryCoordinatorStateStore : CoordinatorStateStore {
    private val groups = ConcurrentHashMap<GroupKey, GroupMetadata>()

    override fun contains(key: GroupKey): Boolean =
        groups.containsKey(key)

    override fun get(key: GroupKey): GroupMetadata? =
        groups[key]

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean =
        if (groups.putIfAbsent(key, group.also { it.storeRevision = 1 }) == null) {
            true
        } else {
            false
        }

    override fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean {
        var deleted = false
        groups.computeIfPresent(key) { _, stored ->
            if (stored.storeRevision == expectedRevision) {
                deleted = true
                null
            } else {
                stored
            }
        }
        return deleted
    }

    override fun save(key: GroupKey, group: GroupMetadata) {
        group.storeRevision += 1
        groups[key] = group
    }

    override fun list(): List<GroupMetadata> =
        groups.values.toList()
}

@Component
@ConditionalOnProperty(prefix = "coordinator.store", name = ["type"], havingValue = "redis")
class RedisCoordinatorStateStore @Autowired constructor(
    private val redisCommands: CoordinatorRedisCommands,
    private val objectMapper: ObjectMapper,
    private val properties: CoordinatorProperties,
) : CoordinatorStateStore {
    constructor(
        redisTemplate: StringRedisTemplate,
        objectMapper: ObjectMapper,
        properties: CoordinatorProperties,
    ) : this(CoordinatorRedisCommands(redisTemplate = redisTemplate), objectMapper, properties)

    private val keys = RedisCoordinatorStateKeys(properties.store.keyPrefix)

    override fun contains(key: GroupKey): Boolean =
        redisCommands.hasKey(keys.forGroup(key).metadata)

    override fun get(key: GroupKey): GroupMetadata? =
        redisCommands.hashGet(keys.forGroup(key).metadata, METADATA_AGGREGATE_FIELD)
            ?.let { objectMapper.readRedisGroupMetadata(it) }

    /**
     * Writes the single group metadata hash atomically when this group does not already exist.
     */
    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean {
        val groupKeys = keys.forGroup(key)
        val stored = writeGroupMetadata(groupKeys, group, onlyIfAbsent = true)
        if (stored) {
            redisCommands.setAdd(keys.groupsIndex, groupKeys.metadata)
        }
        return stored
    }

    /**
     * Removes the single group metadata hash atomically after a failed create/provision rollback.
     */
    override fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean {
        val groupKeys = keys.forGroup(key)
        val deleted = redisCommands.executeLong(
            DELETE_GROUP_IF_REVISION_SCRIPT,
            listOf(groupKeys.metadata),
            expectedRevision.toString(),
        ) == 1L
        if (deleted) {
            redisCommands.setRemove(keys.groupsIndex, groupKeys.metadata)
        }
        return deleted
    }

    /**
     * Replaces the group metadata hash only if the caller's store revision is current.
     */
    override fun save(key: GroupKey, group: GroupMetadata) {
        val groupKeys = keys.forGroup(key)
        writeGroupMetadata(groupKeys, group, onlyIfAbsent = false)
        redisCommands.setAdd(keys.groupsIndex, groupKeys.metadata)
    }

    override fun list(): List<GroupMetadata> =
        redisCommands.setMembers(keys.groupsIndex)
            .mapNotNull { redisCommands.hashGet(it, METADATA_AGGREGATE_FIELD) }
            .map { objectMapper.readRedisGroupMetadata(it) }

    /**
     * Atomically updates the single Redis hash that stores the canonical group metadata.
     */
    private fun writeGroupMetadata(
        keys: RedisCoordinatorGroupKeys,
        group: GroupMetadata,
        onlyIfAbsent: Boolean,
    ): Boolean {
        group.requireSupportedRedisMetadataSchema()
        val previousRevision = group.storeRevision
        val nextRevision = if (onlyIfAbsent) 1 else previousRevision + 1
        group.storeRevision = nextRevision
        val args = mutableListOf(
            if (onlyIfAbsent) "NX" else "UPSERT",
            previousRevision.toString(),
            nextRevision.toString(),
            objectMapper.writeValueAsString(group),
            group.schemaVersion.toString(),
            REDIS_METADATA_LAYOUT_VERSION.toString(),
            group.updatedAt.toString(),
        )

        val result = redisCommands.executeLong(
            UPSERT_GROUP_METADATA_SCRIPT,
            listOf(keys.metadata),
            *args.toTypedArray(),
        )
        return when (result) {
            1L -> true
            0L -> {
                group.storeRevision = previousRevision
                false
            }
            else -> {
                group.storeRevision = previousRevision
                throw CoordinatorStateConflictException(
                    "Redis coordinator metadata changed before save for ${keys.metadata}; expected store revision $previousRevision",
                )
            }
        }
    }

    companion object {
        private const val METADATA_AGGREGATE_FIELD = "aggregate"
        private const val METADATA_REVISION_FIELD = "revision"
        private const val METADATA_SCHEMA_VERSION_FIELD = "schemaVersion"
        private const val METADATA_LAYOUT_VERSION_FIELD = "layoutVersion"
        private const val METADATA_UPDATED_AT_FIELD = "updatedAt"
        private const val REDIS_METADATA_LAYOUT_VERSION = 1

        private val UPSERT_GROUP_METADATA_SCRIPT = DefaultRedisScript(
            """
            if ARGV[1] == 'NX' and redis.call('EXISTS', KEYS[1]) == 1 then
              return 0
            end

            if ARGV[1] ~= 'NX' then
              local currentRevision = redis.call('HGET', KEYS[1], '$METADATA_REVISION_FIELD')
              if currentRevision == false then
                if redis.call('EXISTS', KEYS[1]) == 1 and ARGV[2] ~= '0' then
                  return -1
                end
              elseif currentRevision ~= ARGV[2] then
                return -1
              end
            end

            redis.call(
              'HSET',
              KEYS[1],
              '$METADATA_AGGREGATE_FIELD', ARGV[4],
              '$METADATA_REVISION_FIELD', ARGV[3],
              '$METADATA_SCHEMA_VERSION_FIELD', ARGV[5],
              '$METADATA_LAYOUT_VERSION_FIELD', ARGV[6],
              '$METADATA_UPDATED_AT_FIELD', ARGV[7]
            )
            return 1
            """.trimIndent(),
            Long::class.java,
        )

        private val DELETE_GROUP_IF_REVISION_SCRIPT = DefaultRedisScript(
            """
            local currentRevision = redis.call('HGET', KEYS[1], '$METADATA_REVISION_FIELD')
            if currentRevision == false or currentRevision ~= ARGV[1] then
              return 0
            end

            redis.call('DEL', KEYS[1])
            return 1
            """.trimIndent(),
            Long::class.java,
        )
    }
}

@Component
@ConditionalOnProperty(prefix = "coordinator.store", name = ["type"], havingValue = "jdbc")
class JdbcCoordinatorStateStore @Autowired constructor(
    private val jdbcTemplate: JdbcTemplate,
    private val objectMapper: ObjectMapper,
) : CoordinatorStateStore {
    init {
        jdbcTemplate.execute(CREATE_TABLE_SQL)
    }

    override fun contains(key: GroupKey): Boolean =
        jdbcTemplate.queryForObject(
            """
            SELECT COUNT(*)
            FROM redis_stream_coordinator_metadata
            WHERE stream_prefix = ? AND consumer_group = ?
            """.trimIndent(),
            Long::class.java,
            key.streamPrefix,
            key.consumerGroup,
        )?.let { it > 0 } ?: false

    override fun get(key: GroupKey): GroupMetadata? =
        try {
            jdbcTemplate.queryForObject(
                """
                SELECT metadata_json
                FROM redis_stream_coordinator_metadata
                WHERE stream_prefix = ? AND consumer_group = ?
                """.trimIndent(),
                rowMapper,
                key.streamPrefix,
                key.consumerGroup,
            )
        } catch (_: EmptyResultDataAccessException) {
            null
        }

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean {
        group.requireSupportedRedisMetadataSchema()
        val previousRevision = group.storeRevision
        group.storeRevision = 1
        return try {
            jdbcTemplate.update(
                """
                INSERT INTO redis_stream_coordinator_metadata
                  (stream_prefix, consumer_group, metadata_json, store_revision, schema_version, layout_version, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                key.streamPrefix,
                key.consumerGroup,
                objectMapper.writeValueAsString(group),
                group.storeRevision,
                group.schemaVersion,
                JDBC_METADATA_LAYOUT_VERSION,
                group.updatedAt.toString(),
            )
            true
        } catch (_: DuplicateKeyException) {
            group.storeRevision = previousRevision
            false
        }
    }

    override fun deleteIfRevision(key: GroupKey, expectedRevision: Long): Boolean =
        jdbcTemplate.update(
            """
            DELETE FROM redis_stream_coordinator_metadata
            WHERE stream_prefix = ? AND consumer_group = ? AND store_revision = ?
            """.trimIndent(),
            key.streamPrefix,
            key.consumerGroup,
            expectedRevision,
        ) == 1

    override fun save(key: GroupKey, group: GroupMetadata) {
        group.requireSupportedRedisMetadataSchema()
        val previousRevision = group.storeRevision
        val nextRevision = previousRevision + 1
        group.storeRevision = nextRevision
        val updated = jdbcTemplate.update(
            """
            UPDATE redis_stream_coordinator_metadata
            SET metadata_json = ?, store_revision = ?, schema_version = ?, layout_version = ?, updated_at = ?
            WHERE stream_prefix = ? AND consumer_group = ? AND store_revision = ?
            """.trimIndent(),
            objectMapper.writeValueAsString(group),
            nextRevision,
            group.schemaVersion,
            JDBC_METADATA_LAYOUT_VERSION,
            group.updatedAt.toString(),
            key.streamPrefix,
            key.consumerGroup,
            previousRevision,
        )
        if (updated != 1) {
            group.storeRevision = previousRevision
            throw CoordinatorStateConflictException(
                "JDBC coordinator metadata changed before save for ${key.streamPrefix}:${key.consumerGroup}; expected store revision $previousRevision",
            )
        }
    }

    override fun list(): List<GroupMetadata> =
        jdbcTemplate.query(
            """
            SELECT metadata_json
            FROM redis_stream_coordinator_metadata
            ORDER BY stream_prefix, consumer_group
            """.trimIndent(),
            rowMapper,
        )

    private val rowMapper = RowMapper<GroupMetadata> { rs: ResultSet, _: Int ->
        objectMapper.readRedisGroupMetadata(rs.getString("metadata_json"))
    }

    companion object {
        private const val JDBC_METADATA_LAYOUT_VERSION = 1

        private val CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS redis_stream_coordinator_metadata (
              stream_prefix VARCHAR(512) NOT NULL,
              consumer_group VARCHAR(512) NOT NULL,
              metadata_json CLOB NOT NULL,
              store_revision BIGINT NOT NULL,
              schema_version INTEGER NOT NULL,
              layout_version INTEGER NOT NULL,
              updated_at VARCHAR(64) NOT NULL,
              PRIMARY KEY (stream_prefix, consumer_group)
            )
        """.trimIndent()
    }
}

internal fun ObjectMapper.readRedisGroupMetadata(raw: String): GroupMetadata =
    readValue<GroupMetadata>(normalizeRedisGroupMetadata(raw))
        .also { it.requireSupportedRedisMetadataSchema() }

private fun ObjectMapper.normalizeRedisGroupMetadata(raw: String): String {
    val root = readTree(raw)
    if (!root.isObject) {
        return raw
    }

    val metadata = root as ObjectNode
    var changed = false

    if (!metadata.has("shardCount")) {
        val shardCount = metadata.legacyShardCount()
        if (shardCount != null) {
            metadata.put("shardCount", shardCount)
            changed = true
        }
    }

    if (metadata.normalizeLegacyMigrationShardCounts()) {
        changed = true
    }

    return if (changed) writeValueAsString(metadata) else raw
}

private fun ObjectNode.legacyShardCount(): Int? {
    val shardCounts = legacyShardCountsByVersion()
    val activeCount = get(LEGACY_ACTIVE_WRITE_VERSION_FIELD)
        ?.asString()
        ?.let(shardCounts::get)
        ?.takeIf { it > 0 }
    if (activeCount != null) {
        return activeCount
    }

    val largestDeclaredCount = shardCounts.values.maxOrNull()?.takeIf { it > 0 }
    if (largestDeclaredCount != null) {
        return largestDeclaredCount
    }

    return findValues("shardIndex")
        .map { it.asInt(-1) }
        .maxOrNull()
        ?.plus(1)
        ?.takeIf { it > 0 }
}

private fun ObjectNode.normalizeLegacyMigrationShardCounts(): Boolean {
    val migrations = get("migrations")?.takeIf(JsonNode::isObject)?.asObject() ?: return false
    val shardCounts = legacyShardCountsByVersion()
    val defaultShardCount = get("shardCount")?.asInt(0)?.takeIf { it > 0 }
    var changed = false

    migrations.properties().forEach { (_, migrationNode) ->
        if (!migrationNode.isObject) {
            return@forEach
        }
        val migration = migrationNode as ObjectNode
        if (!migration.has("fromShardCount")) {
            val fromCount = migration.get(LEGACY_FROM_VERSION_FIELD)
                ?.asString()
                ?.let(shardCounts::get)
                ?: defaultShardCount
            if (fromCount != null) {
                migration.put("fromShardCount", fromCount)
                changed = true
            }
        }
        if (!migration.has("toShardCount")) {
            val toCount = migration.get(LEGACY_TO_VERSION_FIELD)
                ?.asString()
                ?.let(shardCounts::get)
                ?: defaultShardCount
            if (toCount != null) {
                migration.put("toShardCount", toCount)
                changed = true
            }
        }
    }

    return changed
}

private fun ObjectNode.legacyShardCountsByVersion(): Map<String, Int> {
    val counts = get(LEGACY_SHARD_COUNTS_BY_VERSION_FIELD)?.takeIf(JsonNode::isObject)?.asObject()
        ?: return emptyMap()
    return counts.properties()
        .mapNotNull { (name, value) -> value.asInt(0).takeIf { it > 0 }?.let { name to it } }
        .toMap()
}

internal fun GroupMetadata.requireSupportedRedisMetadataSchema() {
    if (schemaVersion != COORDINATOR_METADATA_SCHEMA_VERSION) {
        throw CoordinatorStateSchemaException(
            "Unsupported Redis coordinator metadata schemaVersion $schemaVersion; supported schemaVersion is $COORDINATOR_METADATA_SCHEMA_VERSION",
        )
    }
}

private const val LEGACY_VERSION_SUFFIX = "Version"
private const val LEGACY_ACTIVE_WRITE_VERSION_FIELD = "activeWrite$LEGACY_VERSION_SUFFIX"
private const val LEGACY_SHARD_COUNTS_BY_VERSION_FIELD = "shardCountsBy$LEGACY_VERSION_SUFFIX"
private const val LEGACY_FROM_VERSION_FIELD = "from$LEGACY_VERSION_SUFFIX"
private const val LEGACY_TO_VERSION_FIELD = "to$LEGACY_VERSION_SUFFIX"

class RedisCoordinatorStateKeys(
    keyPrefix: String,
) {
    private val prefix = keyPrefix

    val groupsIndex: String = "$prefix:groups"

    fun forGroup(key: GroupKey): RedisCoordinatorGroupKeys {
        val tag = "{${key.streamPrefix}:${key.consumerGroup}}"
        return RedisCoordinatorGroupKeys(
            metadata = "$prefix:$tag:metadata",
        )
    }
}

data class RedisCoordinatorGroupKeys(
    val metadata: String,
)
