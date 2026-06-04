package com.redisstream

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RedisStreamCommandsTemplateTest {
    @Test
    fun `xadd command uses nomkstream so missing scaled-in shards are not recreated`() {
        val args = xAddNomkstreamArguments(
            streamKey = "create-order:17",
            fields = linkedMapOf("eventId" to "e-1", "payload" to "created"),
            maxLen = 10_000,
            approximateTrimming = true,
        ).map { it.toString(Charsets.UTF_8) }

        assertEquals(
            listOf(
                "create-order:17",
                "NOMKSTREAM",
                "MAXLEN",
                "~",
                "10000",
                "*",
                "eventId",
                "e-1",
                "payload",
                "created",
            ),
            args,
        )
    }

    @Test
    fun `xadd command can request exact maxlen trimming`() {
        val args = xAddNomkstreamArguments(
            streamKey = "create-order:0",
            fields = mapOf("payload" to "created"),
            maxLen = 100,
            approximateTrimming = false,
        ).map { it.toString(Charsets.UTF_8) }

        assertEquals("=", args[3])
    }

    @Test
    fun `xadd command rejects blank field names`() {
        assertFailsWith<IllegalArgumentException> {
            xAddNomkstreamArguments(
                streamKey = "create-order:0",
                fields = mapOf("" to "created"),
                maxLen = 100,
                approximateTrimming = true,
            )
        }
    }
}
