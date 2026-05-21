package io.github.ghkdqhrbals.redisstreamcoordinator

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
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
        val stored = writeGroupScopedState(groupKeys, group, onlyIfAbsent = true)
        if (stored) {
            redisTemplate.opsForSet().add(keys.groupsIndex, groupKeys.group)
        }
        return stored
    }

    override fun save(key: GroupKey, group: GroupMetadata) {
        val groupKeys = keys.forGroup(key)
        writeGroupScopedState(groupKeys, group, onlyIfAbsent = false)
        redisTemplate.opsForSet().add(keys.groupsIndex, groupKeys.group)
    }

    override fun list(): List<GroupMetadata> =
        redisTemplate.opsForSet().members(keys.groupsIndex)
            .orEmpty()
            .mapNotNull { redisTemplate.opsForValue().get(it) }
            .map { objectMapper.readValue<GroupMetadata>(it) }

    private fun writeGroupScopedState(
        keys: RedisCoordinatorGroupKeys,
        group: GroupMetadata,
        onlyIfAbsent: Boolean,
    ): Boolean {
        val projection = group.toRedisStateProjection()
        val args = mutableListOf(
            if (onlyIfAbsent) "NX" else "UPSERT",
            objectMapper.writeValueAsString(group),
            projection.activeMigrationId.orEmpty(),
        )
        appendHashArgs(args, projection.members)
        appendHashArgs(args, projection.targetAssignments)
        appendHashArgs(args, projection.currentAssignments)
        appendHashArgs(args, projection.migrations)

        val result = redisTemplate.execute(
            UPSERT_GROUP_STATE_SCRIPT,
            listOf(
                keys.group,
                keys.members,
                keys.targetAssignments,
                keys.currentAssignments,
                keys.migrations,
                keys.activeMigration,
            ),
            *args.toTypedArray(),
        )
        return result == 1L
    }

    private fun appendHashArgs(args: MutableList<String>, values: Map<String, Any>) {
        args += values.size.toString()
        values.forEach { (field, value) ->
            args += field
            args += objectMapper.writeValueAsString(value)
        }
    }

    companion object {
        private val UPSERT_GROUP_STATE_SCRIPT = DefaultRedisScript(
            """
            if ARGV[1] == 'NX' and redis.call('EXISTS', KEYS[1]) == 1 then
              return 0
            end

            redis.call('SET', KEYS[1], ARGV[2])

            local argIndex = 4
            local function replaceHash(key)
              redis.call('DEL', key)
              local count = tonumber(ARGV[argIndex])
              argIndex = argIndex + 1
              for i = 1, count do
                redis.call('HSET', key, ARGV[argIndex], ARGV[argIndex + 1])
                argIndex = argIndex + 2
              end
            end

            replaceHash(KEYS[2])
            replaceHash(KEYS[3])
            replaceHash(KEYS[4])
            replaceHash(KEYS[5])

            if ARGV[3] == '' then
              redis.call('DEL', KEYS[6])
            else
              redis.call('SET', KEYS[6], ARGV[3])
            end

            return 1
            """.trimIndent(),
            Long::class.java,
        )
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
    private val prefix = keyPrefix

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
