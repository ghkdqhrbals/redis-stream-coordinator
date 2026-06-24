package io.github.ghkdqhrbals.redisstreamcoordinator

import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertFalse
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
            val timeIndex = content.indexOf("<th>Time")
            val shardIndex = content.indexOf("<th>Shard")
            val offsetIndex = content.indexOf("""<th class=\"rsc-message-offset\">Offset""")
            val fieldsIndex = content.indexOf("<th>Fields")

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
    fun `stream messages tables expose resizable columns`() {
        val dashboard = readDashboard("redis-stream-coordinator-stream-detail.json")
        val importDashboard = readImportDashboard("redis-stream-coordinator-stream-detail.json")
        val indexHtml = readStaticConsole("index.html")
        val messagesHtml = readStaticConsole("messages.html")
        val appJs = readStaticConsole("app.js")
        val resizeJs = readStaticConsole("table-resize.js")

        listOf(dashboard, importDashboard).forEach { content ->
            assertTrue(content.contains("""data-role": "message-table""") || content.contains("""data-role=\"message-table\""""))
            assertTrue(content.contains("rsc-message-resize-handle"))
            assertTrue(content.contains("function bindColumnResize()"))
            assertTrue(content.contains("redisStreamCoordinator.grafana.messageTableWidths"))
        }
        listOf(indexHtml, messagesHtml).forEach { content ->
            assertTrue(content.contains("resizable-message-table"))
            assertTrue(content.contains("column-resize-handle"))
            assertTrue(content.contains("""/console/table-resize.js"""))
        }
        assertTrue(appJs.contains("window.initResizableTables"))
        assertTrue(resizeJs.contains("function initResizableTables"))
        assertTrue(resizeJs.contains("redisStreamCoordinator.console.tableWidths."))
    }

    @Test
    fun `admin console exposes shard scaling without producer stress controls`() {
        val adminHtml = readStaticConsole("admin.html")
        val adminJs = readStaticConsole("admin.js")
        val signInJs = readStaticConsole("sign-in.js")
        val indexHtml = readStaticConsole("index.html")

        assertTrue(indexHtml.contains("""href="/console/admin.html""""))
        assertTrue(adminHtml.contains("Apply shard scale"))
        assertTrue(adminHtml.contains("Create stream"))
        assertTrue(adminHtml.contains("Coordinator API Session"))
        assertTrue(adminHtml.contains("""id="adminSignOut""""))
        assertTrue(adminHtml.contains("Token expires"))
        assertTrue(adminHtml.contains("Selected Stream"))
        assertTrue(adminHtml.contains("data-admin-shell"))
        assertTrue(adminHtml.contains("admin-request-panel"))
        assertTrue(adminHtml.contains("adminCurlPreview"))
        assertTrue(adminHtml.contains("Copy cURL"))
        assertTrue(adminHtml.contains("adminResponsePreview"))
        assertTrue(signInJs.contains("/coord/v1/auth/login"))
        assertTrue(adminJs.contains("Bearer "))
        assertTrue(adminJs.contains("function logoutAdmin"))
        assertTrue(adminJs.contains("function buildAdminCurl"))
        assertTrue(adminJs.contains("function maskAuthorization"))
        assertTrue(adminJs.contains("function maskSensitivePayload"))
        assertTrue(adminJs.contains("<redacted>"))
        assertTrue(adminJs.contains("renderAdminRequestPreview(path, options, headers)"))
        assertTrue(adminJs.contains("navigator.clipboard.writeText"))
        assertTrue(adminJs.contains("tokenExpiresAt"))
        assertFalse(adminHtml.contains("""id="adminLoginForm""""))
        assertTrue(!adminJs.contains("createBasicAuth"))
        assertTrue(!adminJs.contains("/coord/v1/auth/login"))
        assertTrue(!adminJs.contains("/consumer-concurrency"))
        assertTrue(!adminHtml.contains("Update concurrency policy"))
        assertTrue(!adminHtml.contains("Producer Stress"))
        assertTrue(!adminHtml.contains("Produce stress messages"))
        assertTrue(!adminJs.contains("/sample/stress"))
        assertTrue(!adminJs.contains("handleStressProduce"))
        assertTrue(adminJs.contains("/scale"))
    }

    @Test
    fun `monitoring and message consoles use scalar style request panels`() {
        val indexHtml = readStaticConsole("index.html")
        val messagesHtml = readStaticConsole("messages.html")
        val adminHtml = readStaticConsole("admin.html")
        val styles = readStaticConsole("styles.css")

        assertTrue(indexHtml.contains("data-monitoring-shell"))
        assertTrue(indexHtml.contains("monitorCurlPreview"))
        assertTrue(indexHtml.contains("GET /coord/v1/monitoring/groups"))
        assertFalse(indexHtml.contains("<h2>Health</h2>"))
        assertFalse(indexHtml.contains("healthBadge"))
        assertFalse(indexHtml.contains("redisStatus"))
        assertFalse(indexHtml.contains("loopStatus"))
        assertFalse(indexHtml.contains("<h3>Grafana</h3>"))
        assertFalse(indexHtml.contains("grafanaOverviewLink"))
        assertFalse(indexHtml.contains("grafanaDetailLink"))
        assertFalse(indexHtml.contains("grafanaApiLink"))
        assertTrue(indexHtml.contains("""href="/console/admin.html""""))
        assertTrue(indexHtml.contains("""href="/console/messages.html""""))

        assertTrue(messagesHtml.contains("data-messages-shell"))
        assertTrue(messagesHtml.contains("messageCurlPreview"))
        assertTrue(messagesHtml.contains("Message Operations"))
        assertTrue(messagesHtml.contains("""href="/console/admin.html""""))
        assertTrue(messagesHtml.contains("""href="/console/index.html""""))

        assertTrue(adminHtml.contains("data-admin-shell"))
        assertTrue(adminHtml.contains("""class="admin-sidebar scalar-sidebar""""))
        assertTrue(adminHtml.contains("""href="/console/index.html""""))
        assertTrue(adminHtml.contains("""href="/console/messages.html""""))

        assertTrue(styles.contains(".console-docs-page"))
        assertTrue(styles.contains(".monitor-request-panel"))
        assertTrue(styles.contains(".message-request-panel"))
    }

    @Test
    fun `console pages expose shared side navigation`() {
        val indexHtml = readStaticConsole("index.html")
        val adminHtml = readStaticConsole("admin.html")
        val messagesHtml = readStaticConsole("messages.html")
        val signInHtml = readStaticConsole("sign-in.html")
        val styles = readStaticConsole("styles.css")

        assertTrue(indexHtml.contains("""<nav class="scalar-nav" aria-label="Console">"""))
        assertTrue(indexHtml.contains("""<a class="selected" href="/console/index.html" aria-current="page">Monitoring</a>"""))
        assertTrue(indexHtml.contains("""<a href="/console/admin.html">Admin</a>"""))
        assertTrue(indexHtml.contains("""<a href="/console/messages.html">Messages</a>"""))
        assertTrue(indexHtml.contains("""id="logoutButton" class="ghost-button small""""))

        assertTrue(adminHtml.contains("""<nav class="scalar-nav" aria-label="Console">"""))
        assertTrue(adminHtml.contains("""<a href="/console/index.html">Monitoring</a>"""))
        assertTrue(adminHtml.contains("""<a class="selected" href="/console/admin.html" aria-current="page">Admin</a>"""))
        assertTrue(adminHtml.contains("""<a href="/console/messages.html">Messages</a>"""))
        assertTrue(adminHtml.contains("""id="adminSignOut" class="ghost-button small""""))

        assertTrue(messagesHtml.contains("""<nav class="scalar-nav" aria-label="Console">"""))
        assertTrue(messagesHtml.contains("""<a href="/console/index.html">Monitoring</a>"""))
        assertTrue(messagesHtml.contains("""<a href="/console/admin.html">Admin</a>"""))
        assertTrue(messagesHtml.contains("""<a class="selected" href="/console/messages.html">Messages</a>"""))
        assertTrue(messagesHtml.contains("""id="messageExplorerSignOut" class="ghost-button small""""))

        assertTrue(signInHtml.contains("""<main class="console-auth-shell">"""))
        assertTrue(signInHtml.contains("""<script src="/console/sign-in.js"></script>"""))
        assertTrue(signInHtml.contains("""id="signInNavMonitoring""""))
        assertTrue(signInHtml.contains("""id="signInNavAdmin""""))
        assertTrue(signInHtml.contains("""id="signInNavMessages""""))
        assertTrue(signInHtml.contains("""id="signInForm""""))
        assertFalse(indexHtml.contains("""id="loginForm""""))
        assertFalse(messagesHtml.contains("""id="messageLoginForm""""))
        assertTrue(styles.contains(".console-auth-shell"))
        assertTrue(styles.contains(".console-auth-sidebar"))
        assertTrue(styles.contains(".console-auth-panel"))
        assertTrue(styles.contains("--console-nav-width: 260px;"))
        assertTrue(styles.contains("grid-template-columns: var(--console-nav-width)"))
        assertTrue(styles.contains(".console-docs-shell .topbar h2,\n.admin-topbar h1,\n.message-header h1"))
        assertTrue(styles.contains(""".console-docs-shell .toolbar select,
.console-docs-shell .browser-controls select,
.message-controls select,
.message-login-form input {
    border: 1px solid #d8dee8;
    background: #ffffff;
    color: #202632;
}"""))
        assertTrue(styles.contains(""".console-docs-shell .toolbar select option {
    background: #ffffff;
    color: #202632;
}"""))
        assertTrue(styles.contains(""".message-header {
    border-radius: 0;
    border-width: 0 0 1px;
    background: transparent;
    padding: 0 0 18px;
}"""))
        assertTrue(styles.contains(".console-auth-panel .login-form input"))
        assertTrue(styles.contains(".console-auth-panel .login-form button"))

        assertTrue(styles.contains(""".message-explorer-shell {
    min-height: 100vh;
    padding: 0;
}"""))
        assertTrue(styles.contains(""".message-app {
    grid-template-columns: var(--console-nav-width) minmax(0, 1fr) minmax(340px, 420px);
    gap: 0;
    min-height: 100vh;
    background: #f7f8fb;
}"""))
    }

    @Test
    fun `console pages share persistent auth storage`() {
        val appJs = readStaticConsole("app.js")
        val adminJs = readStaticConsole("admin.js")
        val messagesJs = readStaticConsole("messages.js")
        val signInJs = readStaticConsole("sign-in.js")

        listOf(appJs, adminJs, messagesJs, signInJs).forEach { content ->
            assertTrue(content.contains("redisStreamCoordinator.console.auth"))
            assertTrue(content.contains("redisStreamCoordinator.console.user"))
            assertTrue(content.contains("window.localStorage.getItem(key) || window.sessionStorage.getItem(key)"))
        }
        assertTrue(signInJs.contains("window.localStorage.setItem(key, value)"))
        listOf(appJs, adminJs, messagesJs, signInJs).forEach { content ->
            assertTrue(content.contains("window.localStorage.removeItem(key)"))
            assertTrue(content.contains("window.sessionStorage.removeItem(key)"))
        }
    }

    @Test
    fun `console pages redirect unauthenticated users to shared sign in page`() {
        val appJs = readStaticConsole("app.js")
        val adminJs = readStaticConsole("admin.js")
        val messagesJs = readStaticConsole("messages.js")
        val signInJs = readStaticConsole("sign-in.js")

        listOf(appJs, adminJs, messagesJs).forEach { content ->
            assertTrue(content.contains("""new URL("/console/sign-in.html", window.location.origin)"""))
            assertTrue(content.contains("""url.searchParams.set("section", section)"""))
            assertTrue(content.contains("""url.searchParams.set("next""""))
            assertTrue(content.contains("window.location.pathname"))
            assertTrue(content.contains("window.location.search"))
            assertTrue(content.contains("window.location.hash"))
        }
        assertTrue(appJs.contains("""redirectToSignIn("monitoring")"""))
        assertTrue(adminJs.contains("""redirectToSignIn("admin")"""))
        assertTrue(messagesJs.contains("""redirectToSignIn("messages")"""))
        assertTrue(signInJs.contains("""safeNextPath"""))
        assertTrue(signInJs.contains("""window.location.replace(signInState.next)"""))
        assertTrue(signInJs.contains(""""/coord/v1/auth/login""""))
        assertTrue(signInJs.contains(""""/coord/v1/monitoring/session""""))
    }

    @Test
    fun `grafana dashboards link to admin console and stream producer routing`() {
        listOf(
            "redis-stream-coordinator.json",
            "redis-stream-coordinator-stream-detail.json",
            "redis-stream-coordinator-api.json",
            "redis-stream-coordinator-public.json",
        ).forEach { fileName ->
            val dashboard = readDashboard(fileName)
            val importDashboard = readImportDashboard(fileName)

            assertTrue(dashboard.contains(""""title": "Admin Console""""), "$fileName should link to admin console")
            assertTrue(dashboard.contains("https://coordinator.ghkdqhrbals.org/console/admin.html"), "$fileName should use the production coordinator admin URL")
            assertTrue(importDashboard.contains(""""title": "Admin Console""""), "$fileName import should link to admin console")
            assertTrue(importDashboard.contains("""${'$'}{COORDINATOR_API_URL}/console/admin.html"""), "$fileName import should use the configured coordinator URL")
        }

        listOf(
            readDashboard("redis-stream-coordinator-stream-detail.json"),
            readImportDashboard("redis-stream-coordinator-stream-detail.json"),
        ).forEach { content ->
            assertTrue(content.contains(""""title": "Producer Routing Shards""""))
            assertTrue(content.contains("/coord/v1/streams/${'$'}streamPrefix/producer-routing"))
            assertTrue(!content.contains("/groups/${'$'}consumerGroup/producer-routing"))
            assertTrue(content.contains(""""text": "Stream Key""""))
            assertTrue(content.contains(""""text": "Redis Slot""""))
        }
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
        assertTrue(dashboard.contains("producedPerSecond"))
        assertTrue(dashboard.contains("consumedPerSecond"))
        assertTrue(dashboard.contains(""""selector": "lag""""))
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
    fun `grafana dashboards do not query Prometheus`() {
        listOf(
            "redis-stream-coordinator.json",
            "redis-stream-coordinator-stream-detail.json",
            "redis-stream-coordinator-api.json",
            "redis-stream-coordinator-public.json",
        ).forEach { fileName ->
            val dashboard = readDashboard(fileName)
            val importDashboard = readImportDashboard(fileName)

            listOf(dashboard, importDashboard).forEach { content ->
                assertTrue(!content.contains(""""type": "prometheus""""), "$fileName should not use Prometheus datasource")
                assertTrue(!content.contains(""""uid": "rsc-prometheus""""), "$fileName should not pin the local Prometheus datasource")
                assertTrue(!content.contains("DS_RSC_PROMETHEUS"), "$fileName should not require a Prometheus import datasource")
                assertTrue(!content.contains("last_over_time("), "$fileName should not contain PromQL lookback queries")
                assertTrue(!content.contains("redis_stream_coord_"), "$fileName should not query Prometheus metric names")
            }
        }
    }

    @Test
    fun `api dashboard uses Loki audit logs for latency and request rate`() {
        val dashboard = readDashboard("redis-stream-coordinator-api.json")
        val importDashboard = readImportDashboard("redis-stream-coordinator-api.json")

        listOf(dashboard, importDashboard).forEach { content ->
            assertTrue(content.contains(""""type": "loki""""), "API dashboard should use Loki datasource")
            assertTrue(content.contains("redis-stream-coordinator.audit"), "API dashboard should derive telemetry from audit logs")
            assertTrue(content.contains("durationMs"), "API dashboard should parse audit durationMs")
            assertTrue(content.contains("count_over_time("), "API dashboard should derive request rate from log counts")
            assertTrue(content.contains("quantile_over_time("), "API dashboard should derive p95 latency from log durations")
        }
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

            assertTrue(dashboard.contains(""""name": "DS_RSC_LOKI""""), "$fileName should prompt for Loki datasource")
            assertTrue(dashboard.contains(""""name": "DS_RSC_COORDINATOR_API""""), "$fileName should prompt for Coordinator API datasource")
            assertTrue(dashboard.contains(""""name": "COORDINATOR_API_URL""""), "$fileName should prompt for Coordinator API URL")
            assertTrue(dashboard.contains(""""pluginId": "yesoreyeram-infinity-datasource""""), "$fileName should require Infinity datasource")
            assertTrue(dashboard.contains(""""pluginId": "loki""""), "$fileName should require Loki datasource")
            assertTrue(dashboard.contains(""""value": "http://coordinator:8080""""), "$fileName should have a concrete import default URL")
            assertTrue(dashboard.contains("""${'$'}{DS_RSC_COORDINATOR_API}"""), "$fileName should not pin the local Coordinator API datasource uid")
            assertTrue(dashboard.contains("""${'$'}{COORDINATOR_API_URL}"""), "$fileName should use the import URL input")
            assertTrue(!dashboard.contains(""""uid": "rsc-coordinator-api""""), "$fileName should not pin the local Coordinator API datasource")
            assertTrue(!dashboard.contains(""""uid": "rsc-loki""""), "$fileName should not pin the local Loki datasource")
        }

        val apiDashboard = readImportDashboard("redis-stream-coordinator-api.json")
        assertTrue(apiDashboard.contains("""${'$'}{DS_RSC_LOKI}"""), "API dashboard should not pin the local Loki datasource")
    }

    @Test
    fun `provisioned grafana datasource uses Loki and coordinator API only`() {
        val provisioning = readGrafanaProvisioning("datasources", "datasources.yml")

        assertTrue(provisioning.contains("uid: rsc-loki"))
        assertTrue(provisioning.contains("type: loki"))
        assertTrue(provisioning.contains("isDefault: false"))
        assertTrue(provisioning.contains("uid: rsc-coordinator-api"))
        assertTrue(!provisioning.contains("isDefault: true"))
        assertTrue(!provisioning.contains("uid: rsc-prometheus"))
        assertTrue(!provisioning.contains("type: prometheus"))
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

    private fun readGrafanaProvisioning(vararg pathSegments: String): String {
        val relative = Path.of("monitoring", "grafana", "provisioning", *pathSegments)
        val candidates = listOf(relative, Path.of("..").resolve(relative))
        val path = candidates.firstOrNull(Files::exists)
            ?: error("Grafana provisioning file not found: $relative")
        return Files.readString(path)
    }
}
