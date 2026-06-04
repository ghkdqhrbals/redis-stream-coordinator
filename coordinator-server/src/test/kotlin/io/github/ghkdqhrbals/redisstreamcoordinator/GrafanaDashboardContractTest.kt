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
    fun `stream messages panel shows time shard offset and fields columns`() {
        val dashboard = readDashboard("redis-stream-coordinator-stream-detail.json")
        val importDashboard = readImportDashboard("redis-stream-coordinator-stream-detail.json")

        listOf(dashboard, importDashboard).forEach { content ->
            val timeIndex = content.indexOf("<th>Time</th>")
            val shardIndex = content.indexOf("<th>Shard</th>")
            val offsetIndex = content.indexOf("""<th class=\"rsc-message-offset\">Offset</th>""")
            val fieldsIndex = content.indexOf("<th>Fields</th>")

            assertTrue(timeIndex >= 0)
            assertTrue(timeIndex < shardIndex)
            assertTrue(shardIndex < offsetIndex)
            assertTrue(offsetIndex < fieldsIndex)
            assertTrue(content.contains("function formatRecordTime(row)"))
            assertTrue(content.contains("row.recordTime || row.recordTimestampMs"))
            assertTrue(content.contains("""rsc-message-time"""))
            assertTrue(!content.contains("<th>Payload</th>"))
            assertTrue(!content.contains("""rsc-message-payload"""))
        }
    }

    @Test
    fun `admin console exposes shard scaling and stress controls`() {
        val adminHtml = readStaticConsole("admin.html")
        val adminJs = readStaticConsole("admin.js")
        val indexHtml = readStaticConsole("index.html")

        assertTrue(indexHtml.contains("""href="/console/admin.html""""))
        assertTrue(adminHtml.contains("Apply shard scale"))
        assertTrue(adminHtml.contains("Produce stress messages"))
        assertTrue(!adminJs.contains("/consumer-concurrency"))
        assertTrue(!adminHtml.contains("Update concurrency policy"))
        assertTrue(adminJs.contains("/sample/stress"))
        assertTrue(adminJs.contains("/scale"))
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
        assertTrue(dashboard.contains("function grafanaBasePath()"))
        assertTrue(dashboard.contains("function coordinatorProxyPath(path)"))
        assertTrue(dashboard.contains("coordinatorProxyPath('/coord/v1/monitoring/grafana/shards')"))
        assertTrue(!dashboard.contains("return '/api/datasources/proxy/uid/rsc-coordinator-api"))
        assertTrue(dashboard.contains("function normalizeVariable(value)"))
        assertTrue(dashboard.contains(""""title": "Stream Sharding Overview""""))
        assertTrue(dashboard.contains(""""title": "Produced Rate by Stream""""))
        assertTrue(dashboard.contains(""""title": "Consumed Rate by Stream""""))
        assertTrue(dashboard.contains("""{{stream}} / {{group}} lag"""))
        assertTrue(dashboard.contains(""""h": 24"""))
        assertTrue(dashboard.contains(""""overflow": "auto""""))
        assertTrue(dashboard.contains(""""y": 12"""))
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

    @Test
    fun `api dashboard latency panels use recent window instead of sticky full-range maxima`() {
        val dashboard = readDashboard("redis-stream-coordinator-api.json")

        assertTrue(dashboard.contains("redis_stream_coord_api_request_duration_seconds"))
        assertTrue(dashboard.contains("[5m]"), "API latency should represent a recent operational window")
        assertTrue(
            !Regex("""redis_stream_coord_api_request_duration_seconds[^"\n]+?\[\${'$'}__range]""").containsMatchIn(dashboard),
            "API latency should not keep old deployment spikes for the whole selected range",
        )
    }

    @Test
    fun `html graphics datasource calls are grafana subpath safe`() {
        listOf(
            "redis-stream-coordinator.json",
            "redis-stream-coordinator-stream-detail.json",
        ).forEach { fileName ->
            val dashboard = readDashboard(fileName)

            assertTrue(dashboard.contains("function grafanaBasePath()"), "$fileName should derive Grafana base path")
            assertTrue(dashboard.contains("function coordinatorProxyPath(path)"), "$fileName should route through datasource proxy helper")
            assertTrue(!dashboard.contains("fetch('/api/datasources/proxy"), "$fileName should not fetch root-relative proxy URLs")
            assertTrue(!dashboard.contains("const proxyPath = '/api/datasources/proxy"), "$fileName should not pin root-relative proxy URLs")
            assertTrue(!dashboard.contains("return '/api/datasources/proxy"), "$fileName should not return root-relative proxy URLs")
        }
    }

    @Test
    fun `import dashboards expose datasource and coordinator URL inputs`() {
        listOf(
            "redis-stream-coordinator.json",
            "redis-stream-coordinator-stream-detail.json",
            "redis-stream-coordinator-api.json",
            "redis-stream-coordinator-public.json",
        ).forEach { fileName ->
            val dashboard = readImportDashboard(fileName)

            assertTrue(dashboard.contains(""""name": "DS_RSC_PROMETHEUS""""), "$fileName should prompt for Prometheus datasource")
            assertTrue(dashboard.contains(""""name": "DS_RSC_COORDINATOR_API""""), "$fileName should prompt for Coordinator API datasource")
            assertTrue(dashboard.contains(""""name": "COORDINATOR_API_URL""""), "$fileName should prompt for Coordinator API URL")
            assertTrue(dashboard.contains(""""pluginId": "yesoreyeram-infinity-datasource""""), "$fileName should require Infinity datasource")
            assertTrue(dashboard.contains(""""pluginId": "prometheus""""), "$fileName should require Prometheus datasource")
            assertTrue(dashboard.contains(""""value": "http://coordinator:8080""""), "$fileName should have a concrete import default URL")
            assertTrue(dashboard.contains("""${'$'}{DS_RSC_COORDINATOR_API}"""), "$fileName should not pin the local Coordinator API datasource uid")
            assertTrue(dashboard.contains("""${'$'}{COORDINATOR_API_URL}"""), "$fileName should use the import URL input")
            assertTrue(!dashboard.contains(""""uid": "rsc-coordinator-api""""), "$fileName should not pin the local Coordinator API datasource")
            assertTrue(!dashboard.contains(""""uid": "rsc-prometheus""""), "$fileName should not pin the local Prometheus datasource")
        }
    }

    @Test
    fun `public dashboard is compatible with external sharing`() {
        val dashboard = readDashboard("redis-stream-coordinator-public.json")
        val importDashboard = readImportDashboard("redis-stream-coordinator-public.json")

        assertTrue(dashboard.contains(""""uid": "redis-stream-coordinator-public""""))
        assertTrue(dashboard.contains(""""title": "Redis Stream Coordinator Public Overview""""))
        assertTrue(dashboard.contains(""""title": "Stream Groups""""))
        assertTrue(dashboard.contains(""""title": "Shard Inventory""""))
        assertTrue(!dashboard.contains("gapit-htmlgraphics-panel"))
        assertTrue(!dashboard.contains("htmlGraphics"))
        assertTrue(!dashboard.contains("onInit"))
        assertTrue(!dashboard.contains("fetch("))
        assertTrue(!importDashboard.contains("gapit-htmlgraphics-panel"))
        assertTrue(!importDashboard.contains(""""id": "gapit-htmlgraphics-panel""""))
        assertTrue(importDashboard.contains(""""parser": "backend""""))
        assertTrue(importDashboard.contains("""${'$'}{DS_RSC_PROMETHEUS}"""))
        assertTrue(importDashboard.contains("""${'$'}{DS_RSC_COORDINATOR_API}"""))
        assertTrue(importDashboard.contains("""${'$'}{COORDINATOR_API_URL}/coord/v1/monitoring/grafana/groups"""))
        assertTrue(importDashboard.contains("""${'$'}{COORDINATOR_API_URL}/coord/v1/monitoring/grafana/shards?streamPrefix=&consumerGroup="""))
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

    private fun readImportDashboard(fileName: String): String {
        val candidates = listOf(
            Path.of("monitoring", "grafana", "import", fileName),
            Path.of("..", "monitoring", "grafana", "import", fileName),
        )
        val path = candidates.firstOrNull(Files::exists)
            ?: error("Import dashboard file not found: $fileName")
        return Files.readString(path)
    }

    private fun readStaticConsole(fileName: String): String {
        val candidates = listOf(
            Path.of("coordinator-server", "src", "main", "resources", "static", "console", fileName),
            Path.of("..", "coordinator-server", "src", "main", "resources", "static", "console", fileName),
        )
        val path = candidates.firstOrNull(Files::exists)
            ?: error("Static console file not found: $fileName")
        return Files.readString(path)
    }
}
