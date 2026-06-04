package com.redisstream.consumer

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RedisStreamCommandCompatibilityTest {
    @Test
    fun `auto acknowledgement uses xackdel only when redis supports it`() {
        assertEquals(
            RedisStreamResolvedAckMode.XACK,
            RedisStreamCommandCompatibility.resolveAckMode(
                RedisStreamAckMode.AUTO,
                RedisStreamCommandSupport.forRedisVersion(RedisServerVersion.parse("7.4.9")),
            ),
        )
        assertEquals(
            RedisStreamResolvedAckMode.XACKDEL,
            RedisStreamCommandCompatibility.resolveAckMode(
                RedisStreamAckMode.AUTO,
                RedisStreamCommandSupport.forRedisVersion(RedisServerVersion.parse("8.2.0")),
            ),
        )
    }

    @Test
    fun `explicit xackdel fails before unsupported command is sent`() {
        assertFailsWith<IllegalArgumentException> {
            RedisStreamCommandCompatibility.resolveAckMode(
                RedisStreamAckMode.XACKDEL,
                RedisStreamCommandSupport.forRedisVersion(RedisServerVersion.parse("8.0.0")),
            )
        }
    }

    @Test
    fun `xnack is guarded by redis 8_8 capability`() {
        assertFalse(RedisStreamCommandSupport.forRedisVersion(RedisServerVersion.parse("8.6.3")).supportsXNack)
        assertTrue(RedisStreamCommandSupport.forRedisVersion(RedisServerVersion.parse("8.8.0")).supportsXNack)
        assertFailsWith<IllegalArgumentException> {
            RedisStreamCommandCompatibility.validateFailureMode(
                RedisStreamFailureMode.XNACK,
                RedisStreamCommandSupport.forRedisVersion(RedisServerVersion.parse("8.6.3")),
            )
        }
    }
}
