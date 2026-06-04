package io.github.ghkdqhrbals.redisstreamcoordinator

import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertTrue

class GrafanaDashboardContractTest {
    @Test
    fun `stream messages panel wires bounded pagination controls`() {
        val dashboard = readDashboard("redis-stream-coordinator-stream-detail.json")

        assertTrue(dashboard.contains("""data-action=\"first\""""))
        assertTrue(dashboard.contains("""data-action=\"last\""""))
        assertTrue(dashboard.contains("function pageCount("))
        assertTrue(dashboard.contains("action('first').addEventListener"))
        assertTrue(dashboard.contains("action('last').addEventListener"))
        assertTrue(dashboard.contains("String(currentPage) + ' / '"))
        assertTrue(dashboard.contains("__rsc_last__"))
        assertTrue(dashboard.contains("function tailCursor("))
        assertTrue(dashboard.contains("function tailCursorDistance("))
        assertTrue(dashboard.contains("tailCursor(distance + 1)"))
        assertTrue(dashboard.contains("tailCursor(0)"))
    }

    @Test
    fun `overview dashboard keeps group and owner columns operator friendly`() {
        val dashboard = readDashboard("redis-stream-coordinator.json")
        val titleIndex = dashboard.indexOf(""""title": "Coordinator Groups Table"""")
        val table = dashboard.substring(
            dashboard.lastIndexOf(""""targets": [""", titleIndex),
            dashboard.indexOf(""""title": "Shard Offsets & Memory""""),
        )

        val streamIndex = table.indexOf(""""text": "Stream"""")
        val groupIndex = table.indexOf(""""text": "Group"""")
        val assignedShardsIndex = table.indexOf(""""text": "Assigned / Shards"""")
        val membersIndex = table.indexOf(""""text": "Members"""")
        val lagIndex = table.indexOf(""""text": "Lag"""")
        val pendingIndex = table.indexOf(""""text": "Pending"""")

        assertTrue(streamIndex >= 0)
        assertTrue(streamIndex < groupIndex)
        assertTrue(groupIndex < assignedShardsIndex)
        assertTrue(assignedShardsIndex < membersIndex)
        assertTrue(membersIndex < lagIndex)
        assertTrue(lagIndex < pendingIndex)
        assertTrue(!table.contains(""""text": "Assigned""""))
        assertTrue(!table.contains(""""text": "Shards""""))
        assertTrue(dashboard.contains("currentOwnerMemberIds"))
        assertTrue(dashboard.contains("targetOwnerMemberIds"))
        assertTrue(dashboard.contains("ownerState"))
    }

    @Test
    fun `stream sharding overview groups consumer groups under each stream`() {
        val dashboard = readDashboard("redis-stream-coordinator.json")

        assertTrue(dashboard.contains("rsc-stream-section"))
        assertTrue(dashboard.contains("rsc-stream-groups"))
        assertTrue(dashboard.contains("rsc-group-row"))
        assertTrue(dashboard.contains("const streamGroups = new Map()"))
        assertTrue(dashboard.contains("stream.groups.map(function (group)"))
        assertTrue(dashboard.contains("renderGroup(group, shardsByGroup, nodeSizeByGroup)"))
        assertTrue(dashboard.contains("onInit"))
        assertTrue(dashboard.contains("/api/datasources/proxy/uid/rsc-coordinator-api/coord/v1/monitoring/grafana/shards"))
        assertTrue(dashboard.contains("function normalizeVariable(value)"))
        assertTrue(dashboard.contains(""""title": "Stream Sharding Overview""""))
        assertTrue(dashboard.contains(""""h": 24"""))
        assertTrue(dashboard.contains(""""overflow": "auto""""))
        assertTrue(dashboard.contains(""""y": 28"""))
        assertTrue(dashboard.contains("position: relative; display: grid"))
        assertTrue(dashboard.contains(".rsc-hover-detail { position: absolute;"))
        assertTrue(dashboard.contains("const rootRect = root.getBoundingClientRect()"))
        assertTrue(dashboard.contains("event.clientX - rootRect.left"))
        assertTrue(dashboard.contains("function showHoverDetail(event, shardNode)"))
        assertTrue(dashboard.contains("shardNode.addEventListener('mousemove', function (event) { showHoverDetail(event, shardNode); })"))
        assertTrue(!dashboard.contains("""title=\"' + esc(title)"""))
    }

    @Test
    fun `grafana gauge queries carry previous values across dashboard range`() {
        listOf(
            "redis-stream-coordinator.json",
            "redis-stream-coordinator-stream-detail.json",
            "redis-stream-coordinator-api.json",
        ).forEach { fileName ->
            val dashboard = readDashboard(fileName)

            assertTrue(dashboard.contains("last_over_time("), "$fileName should use Prometheus lookback for gauge graphs")
            assertTrue(dashboard.contains("[${'$'}__range]"), "$fileName should carry previous gauge samples from the dashboard range")
            assertTrue(
                !Regex("""last_over_time\([^"\n]+?\[2m]""").containsMatchIn(dashboard),
                "$fileName should not use the old short gauge lookback window",
            )
            assertTrue(!dashboard.contains("[1m]"), "$fileName should not use the old one-minute rate window")
        }
    }

    private fun readDashboard(fileName: String): String {
        val candidates = listOf(
            Path.of("monitoring", "grafana", "dashboards", fileName),
            Path.of("..", "monitoring", "grafana", "dashboards", fileName),
        )
        val path = candidates.firstOrNull(Files::exists)
            ?: error("Dashboard file not found: $fileName")
        return Files.readString(path)
    }
}
