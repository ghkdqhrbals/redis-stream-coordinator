package com.redisstream.consumer

import com.redisstream.protocol.CoordinatorProtocol
import kotlin.test.Test
import kotlin.test.assertEquals

class CoordinatorConsumerProtocolDefaultsTest {
    @Test
    fun `consumer defaults come from shared coordination protocol`() {
        val consumer = CoordinatorConsumerProperties()
        val defaults = CoordinatorProtocol.DEFAULT_TIMING

        assertEquals(defaults.heartbeatInterval, consumer.heartbeatInterval)
        assertEquals(defaults.rebalanceTimeout, consumer.rebalanceTimeout)
    }
}
