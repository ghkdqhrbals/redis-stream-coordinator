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
    override fun contains(key: GroupKey): Boolean =
        redisTemplate.hasKey(groupKey(key))

    override fun get(key: GroupKey): GroupMetadata? =
        redisTemplate.opsForValue().get(groupKey(key))?.let { objectMapper.readValue<GroupMetadata>(it) }

    override fun putIfAbsent(key: GroupKey, group: GroupMetadata): Boolean {
        val stored = redisTemplate.opsForValue().setIfAbsent(groupKey(key), objectMapper.writeValueAsString(group))
        if (stored == true) {
            redisTemplate.opsForSet().add(groupsIndexKey(), groupKey(key))
        }
        return stored == true
    }

    override fun save(key: GroupKey, group: GroupMetadata) {
        redisTemplate.opsForValue().set(groupKey(key), objectMapper.writeValueAsString(group))
        redisTemplate.opsForSet().add(groupsIndexKey(), groupKey(key))
    }

    override fun list(): List<GroupMetadata> =
        redisTemplate.opsForSet().members(groupsIndexKey())
            .orEmpty()
            .mapNotNull { redisTemplate.opsForValue().get(it) }
            .map { objectMapper.readValue<GroupMetadata>(it) }

    private fun groupKey(key: GroupKey): String =
        "${properties.store.keyPrefix}:{${key.streamPrefix}:${key.consumerGroup}}:group"

    private fun groupsIndexKey(): String =
        "${properties.store.keyPrefix}:groups"
}
