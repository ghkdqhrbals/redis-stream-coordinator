package io.github.ghkdqhrbals.redisstreamcoordinator

import com.redisstream.protocol.CoordinatorProtocol
import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import kotlin.test.Test
import kotlin.test.assertEquals

class CoordinatorProtocolDefaultsTest {
    @Test
    fun `coordinator defaults come from shared coordination protocol`() {
        val coordinator = CoordinatorProperties()
        val defaults = CoordinatorProtocol.DEFAULT_TIMING

        assertEquals(defaults.heartbeatInterval, coordinator.heartbeatInterval)
        assertEquals(defaults.memberLeaseTtl, coordinator.memberLeaseTtl)
    }
}
