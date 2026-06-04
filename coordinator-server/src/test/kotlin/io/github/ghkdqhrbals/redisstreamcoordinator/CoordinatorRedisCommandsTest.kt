package io.github.ghkdqhrbals.redisstreamcoordinator

import io.github.ghkdqhrbals.redisstreamcoordinator.redis.CoordinatorRedisCommands
import io.lettuce.core.RedisFuture
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands
import io.lettuce.core.protocol.CommandArgs
import io.lettuce.core.protocol.CommandType
import org.mockito.Mockito
import org.springframework.data.redis.connection.RedisConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import java.lang.reflect.Proxy
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class CoordinatorRedisCommandsTest {
    @Test
    fun `memory usage routes stream key as the cluster key`() {
        val factory = Mockito.mock(RedisConnectionFactory::class.java)
        val connection = Mockito.mock(RedisConnection::class.java)
        @Suppress("UNCHECKED_CAST")
        val future = Mockito.mock(RedisFuture::class.java) as RedisFuture<Long>
        val dispatchedCommand = AtomicReference<Any?>()
        val dispatchedArgs = AtomicReference<String>()
        val clusterCommands = clusterCommandsProxy(future, dispatchedCommand, dispatchedArgs)

        Mockito.`when`(factory.connection).thenReturn(connection)
        Mockito.`when`(connection.nativeConnection).thenReturn(clusterCommands)
        Mockito.`when`(future.get()).thenReturn(4096L)

        val result = CoordinatorRedisCommands(redisConnectionFactory = factory).memoryUsage("create-order:0")

        assertEquals(4096L, result)
        assertEquals(CommandType.MEMORY, dispatchedCommand.get())
        assertEquals("USAGE key<create-order:0>", dispatchedArgs.get())
        Mockito.verify(connection).close()
    }

    @Test
    fun `cluster slot owners resolve stream slots to master nodes`() {
        val factory = Mockito.mock(RedisConnectionFactory::class.java)
        val connection = Mockito.mock(RedisConnection::class.java)
        @Suppress("UNCHECKED_CAST")
        val memoryFuture = Mockito.mock(RedisFuture::class.java) as RedisFuture<Long>
        @Suppress("UNCHECKED_CAST")
        val slotsFuture = Mockito.mock(RedisFuture::class.java) as RedisFuture<List<Any>>
        val dispatchedCommand = AtomicReference<Any?>()
        val dispatchedArgs = AtomicReference<String>()
        val clusterCommands = clusterCommandsProxy(memoryFuture, dispatchedCommand, dispatchedArgs, slotsFuture)

        Mockito.`when`(factory.connection).thenReturn(connection)
        Mockito.`when`(connection.nativeConnection).thenReturn(clusterCommands)
        Mockito.`when`(slotsFuture.get()).thenReturn(
            listOf(
                listOf(0L, 5_460L, listOf("10.0.0.10", 7001L, "node-a")),
                listOf(5_461L, 10_922L, listOf("10.0.0.11", 7002L, "node-b")),
                listOf(10_923L, 16_383L, listOf("10.0.0.12", 7003L, "node-c")),
            ),
        )

        val owners = CoordinatorRedisCommands(redisConnectionFactory = factory).clusterSlotOwners(listOf(0, 9_192, 13_133))

        assertEquals("10.0.0.10:7001", owners.getValue(0).endpoint)
        assertEquals("node-a", owners.getValue(0).nodeId)
        assertEquals(0, owners.getValue(0).slotRangeStart)
        assertEquals(5_460, owners.getValue(0).slotRangeEnd)
        assertEquals("10.0.0.11:7002", owners.getValue(9_192).endpoint)
        assertEquals("10.0.0.12:7003", owners.getValue(13_133).endpoint)
        assertNull(dispatchedCommand.get())
        Mockito.verify(connection).close()
    }

    private fun clusterCommandsProxy(
        future: RedisFuture<Long>,
        dispatchedCommand: AtomicReference<Any?>,
        dispatchedArgs: AtomicReference<String>,
        slotsFuture: RedisFuture<List<Any>>? = null,
    ): RedisClusterAsyncCommands<ByteArray, ByteArray> {
        val proxy = Proxy.newProxyInstance(
            RedisClusterAsyncCommands::class.java.classLoader,
            arrayOf(RedisClusterAsyncCommands::class.java),
        ) { _, method, arguments ->
            when (method.name) {
                "dispatch" -> {
                    dispatchedCommand.set(arguments?.getOrNull(0))
                    @Suppress("UNCHECKED_CAST")
                    val args = arguments?.getOrNull(2) as CommandArgs<ByteArray, ByteArray>
                    dispatchedArgs.set(args.toCommandString())
                    future
                }
                "clusterSlots" -> slotsFuture ?: error("Unexpected clusterSlots command")
                "isOpen" -> true
                "toString" -> "RedisClusterAsyncCommandsProxy"
                else -> error("Unexpected Redis command method: ${method.name}")
            }
        }

        @Suppress("UNCHECKED_CAST")
        return proxy as RedisClusterAsyncCommands<ByteArray, ByteArray>
    }
}
