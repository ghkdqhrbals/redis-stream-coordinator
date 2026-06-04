package com.redisstream.protocol

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CoordinatorProtocolTest {
    @Test
    fun `current coordination version owns timing defaults`() {
        val defaults = CoordinatorProtocol.timingDefaults(CoordinatorProtocol.CURRENT_COORDINATION_VERSION)

        assertEquals(CoordinatorProtocol.DEFAULT_TIMING, defaults)
        assertEquals(3_000, defaults.heartbeatInterval.toMillis())
        assertEquals(15_000, defaults.memberLeaseTtl.toMillis())
        assertEquals(60_000, defaults.rebalanceTimeout.toMillis())
        assertTrue(defaults.memberLeaseTtl > defaults.heartbeatInterval)
    }

    @Test
    fun `documented version metadata carries the same timing defaults`() {
        val version = CoordinatorProtocol.VERSIONS.single { it.version == CoordinatorProtocol.CURRENT_COORDINATION_VERSION }

        assertEquals(CoordinatorProtocol.DEFAULT_TIMING, version.timingDefaults)
        assertTrue(CoordinatorProtocol.support(version.version))
    }
}
