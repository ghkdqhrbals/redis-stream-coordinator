package io.github.ghkdqhrbals.redisstreamcoordinator

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.readValue
import java.util.concurrent.ConcurrentHashMap

interface CoordinatorStateStore {
    fun contains(key: GroupKey): Boolean
    fun get(key: GroupKey): GroupMetadata?
    fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean
    fun save(key: GroupKey, group: GroupMetadata)
    fun list(): List<GroupMetadata>
}

@Component
@ConditionalOnProperty(prefix = "coordinator.store", name = ["type"], havingValue = "memory", matchIfMissing = true)
class InMemoryCoordinatorStateStore : CoordinatorStateStore {
    private val groups = ConcurrentHashMap<GroupKey, GroupMetadata>()

    override fun contains(key: GroupKey): Boolean =
        groups.containsKey(key)

    override fun get(key: GroupKey): GroupMetadata? =
        groups[key]

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean =
        groups.putIfAbsent(key, group) == null

    override fun save(key: GroupKey, group: GroupMetadata) {
        groups[key] = group
    }

    override fun list(): List<GroupMetadata> =
        groups.values.toList()
}

@Component
@ConditionalOnProperty(prefix = "coordinator.store", name = ["type"], havingValue = "redis")
class RedisCoordinatorStateStore(
    private val redisTemplate: StringRedisTemplate,
    private val objectMapper: ObjectMapper,
    private val properties: CoordinatorProperties,
) : CoordinatorStateStore {
    private val keys = RedisCoordinatorStateKeys(properties.store.keyPrefix)

    override fun contains(key: GroupKey): Boolean =
        redisTemplate.hasKey(keys.forGroup(key).group)

    override fun get(key: GroupKey): GroupMetadata? =
        redisTemplate.opsForValue().get(keys.forGroup(key).group)?.let { objectMapper.readValue<GroupMetadata>(it) }

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean {
        val groupKeys = keys.forGroup(key)
        val stored = redisTemplate.opsForValue().setIfAbsent(groupKeys.group, objectMapper.writeValueAsString(group))
        if (stored == true) {
            redisTemplate.opsForSet().add(keys.groupsIndex, groupKeys.group)
            writeProjectedKeys(groupKeys, group)
        }
        return stored == true
    }

    override fun save(key: GroupKey, group: GroupMetadata) {
        val groupKeys = keys.forGroup(key)
        redisTemplate.opsForValue().set(groupKeys.group, objectMapper.writeValueAsString(group))
        redisTemplate.opsForSet().add(keys.groupsIndex, groupKeys.group)
        writeProjectedKeys(groupKeys, group)
    }

    override fun list(): List<GroupMetadata> =
        redisTemplate.opsForSet().members(keys.groupsIndex)
            .orEmpty()
            .mapNotNull { redisTemplate.opsForValue().get(it) }
            .map { objectMapper.readValue<GroupMetadata>(it) }

    private fun writeProjectedKeys(keys: RedisCoordinatorGroupKeys, group: GroupMetadata) {
        val projection = group.toRedisStateProjection()
        replaceHash(keys.members, projection.members)
        replaceHash(keys.targetAssignments, projection.targetAssignments)
        replaceHash(keys.currentAssignments, projection.currentAssignments)
        replaceHash(keys.migrations, projection.migrations)

        projection.activeMigrationId?.let {
            redisTemplate.opsForValue().set(keys.activeMigration, it)
        } ?: redisTemplate.delete(keys.activeMigration)
    }

    private fun replaceHash(key: String, values: Map<String, Any>) {
        redisTemplate.delete(key)
        if (values.isNotEmpty()) {
            redisTemplate.opsForHash<String, String>().putAll(
                key,
                values.mapValues { (_, value) -> objectMapper.writeValueAsString(value) },
            )
        }
    }
}

data class RedisCoordinatorStateProjection(
    val members: Map<String, MemberMetadata>,
    val targetAssignments: Map<String, Set<ShardId>>,
    val currentAssignments: Map<String, Set<ShardId>>,
    val migrations: Map<String, Migration>,
    val activeMigrationId: String?,
)

fun GroupMetadata.toRedisStateProjection(): RedisCoordinatorStateProjection =
    RedisCoordinatorStateProjection(
        members = members.toSortedMap(),
        targetAssignments = targetAssignments
            .mapValues { (_, shards) -> shards.toSortedSet() }
            .toSortedMap(),
        currentAssignments = members
            .mapValues { (_, member) -> member.currentAssignment.toSortedSet() }
            .toSortedMap(),
        migrations = migrations.toSortedMap(),
        activeMigrationId = activeMigrationId,
    )

class RedisCoordinatorStateKeys(
    keyPrefix: String,
) {
    private val prefix = keyPrefix.trimEnd(':')

    val groupsIndex: String = "$prefix:groups"

    fun forGroup(key: GroupKey): RedisCoordinatorGroupKeys {
        val tag = "{${key.streamPrefix}:${key.consumerGroup}}"
        return RedisCoordinatorGroupKeys(
            group = "$prefix:$tag:group",
            members = "$prefix:$tag:members",
            targetAssignments = "$prefix:$tag:target-assignments",
            currentAssignments = "$prefix:$tag:current-assignments",
            migrations = "$prefix:$tag:migrations",
            activeMigration = "$prefix:$tag:active-migration",
        )
    }
}

data class RedisCoordinatorGroupKeys(
    val group: String,
    val members: String,
    val targetAssignments: String,
    val currentAssignments: String,
    val migrations: String,
    val activeMigration: String,
)
