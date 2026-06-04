const API_BASE = "/coord/v1/monitoring";
const GRAFANA_BASE = "/coord/v1/monitoring/grafana";
const AUTH_KEY = "redisStreamCoordinator.console.auth";
const USER_KEY = "redisStreamCoordinator.console.user";
const REQUEST_TIMEOUT_MS = 5000;

const state = {
    authHeader: readSession(AUTH_KEY),
    username: readSession(USER_KEY),
    groups: [],
    selectedKey: null,
    refreshTimer: null,
    selectedMessageShard: null,
    messageCursor: null,
    grafanaGroups: [],
    grafanaShards: [],
    globalMessages: {
        streamPrefix: "",
        consumerGroup: "",
        shardIndex: "all",
        cursors: [""],
        page: 0,
        nextCursor: "",
        totalMessages: null,
    },
};

const elements = {};

document.addEventListener("DOMContentLoaded", () => {
    bindElements();
    bindEvents();

    if (state.authHeader) {
        showApp();
        refreshAll();
        scheduleRefresh();
    } else {
        showLogin();
    }
});

function bindElements() {
    [
        "loginView",
        "appView",
        "loginForm",
        "username",
        "password",
        "loginButton",
        "loginError",
        "logoutButton",
        "refreshButton",
        "autoRefresh",
        "refreshInterval",
        "coordinatorId",
        "healthBadge",
        "redisStatus",
        "loopStatus",
        "groupList",
        "pageTitle",
        "pageSubtitle",
        "lastUpdated",
        "emptyState",
        "consoleOverview",
        "globalCoordinatorSignal",
        "globalActiveConsumers",
        "globalStreamEntries",
        "globalStreamMemory",
        "globalTotalLag",
        "globalPendingEntries",
        "overviewShardCount",
        "streamOverview",
        "grafanaGroupCount",
        "grafanaGroupsTable",
        "globalMessageStream",
        "globalMessageGroup",
        "globalMessageShard",
        "globalMessageLimit",
        "globalMessageFirst",
        "globalMessagePrev",
        "globalMessageNext",
        "globalMessageLast",
        "globalMessageRefresh",
        "globalMessageStatus",
        "globalMessageRows",
        "apiMetricStatus",
        "apiMetricList",
        "grafanaOverviewLink",
        "grafanaDetailLink",
        "grafanaApiLink",
        "detailView",
        "memberSignal",
        "memberSignalHint",
        "shardSignal",
        "shardSignalHint",
        "revokingSignal",
        "pendingSignal",
        "heartbeatSignal",
        "heartbeatSignalHint",
        "groupState",
        "metadataVersion",
        "groupEpoch",
        "assignmentEpoch",
        "shardCount",
        "memberCount",
        "membersTable",
        "assignmentStatus",
        "shardMatrix",
        "messageShardSelect",
        "messageRefreshButton",
        "messageBrowser",
        "workloadCount",
        "memberWorkload",
        "assignmentGrid",
        "progressCount",
        "progressList",
        "migrationStatus",
        "migrationList",
    ].forEach((id) => {
        elements[id] = document.getElementById(id);
    });
}

function bindEvents() {
    elements.loginForm.addEventListener("submit", handleLogin);
    elements.logoutButton.addEventListener("click", logout);
    elements.refreshButton.addEventListener("click", refreshAll);
    elements.messageRefreshButton.addEventListener("click", () => loadMessages(true));
    elements.messageShardSelect.addEventListener("change", () => {
        state.selectedMessageShard = elements.messageShardSelect.value;
        state.messageCursor = null;
        loadMessages(true);
    });
    elements.globalMessageStream.addEventListener("change", () => {
        state.globalMessages.streamPrefix = elements.globalMessageStream.value;
        state.globalMessages.consumerGroup = "";
        state.globalMessages.shardIndex = "all";
        resetGlobalMessages();
        renderGlobalMessageSelectors();
        loadGlobalMessages();
    });
    elements.globalMessageGroup.addEventListener("change", () => {
        state.globalMessages.consumerGroup = elements.globalMessageGroup.value;
        state.globalMessages.shardIndex = "all";
        resetGlobalMessages();
        renderGlobalMessageSelectors();
        loadGlobalMessages();
    });
    elements.globalMessageShard.addEventListener("change", () => {
        state.globalMessages.shardIndex = elements.globalMessageShard.value || "all";
        resetGlobalMessages();
        loadGlobalMessages();
    });
    elements.globalMessageLimit.addEventListener("change", () => {
        resetGlobalMessages();
        loadGlobalMessages();
    });
    elements.globalMessageRefresh.addEventListener("click", () => {
        resetGlobalMessages();
        loadGlobalMessages();
    });
    elements.globalMessageFirst.addEventListener("click", () => {
        resetGlobalMessages();
        loadGlobalMessages();
    });
    elements.globalMessagePrev.addEventListener("click", () => {
        if (state.globalMessages.page <= 0) {
            return;
        }
        state.globalMessages.page -= 1;
        state.globalMessages.nextCursor = "";
        loadGlobalMessages();
    });
    elements.globalMessageNext.addEventListener("click", () => {
        if (!state.globalMessages.nextCursor) {
            return;
        }
        state.globalMessages.page += 1;
        state.globalMessages.cursors[state.globalMessages.page] = state.globalMessages.nextCursor;
        state.globalMessages.nextCursor = "";
        loadGlobalMessages();
    });
    elements.globalMessageLast.addEventListener("click", () => {
        const total = Number(state.globalMessages.totalMessages);
        const limit = Number(elements.globalMessageLimit.value || 25);
        const lastPage = Number.isFinite(total) && total > 0 ? Math.max(0, Math.ceil(total / limit) - 1) : 0;
        state.globalMessages.page = lastPage;
        state.globalMessages.cursors[state.globalMessages.page] = "__rsc_tail__:0";
        state.globalMessages.nextCursor = "";
        loadGlobalMessages();
    });
    elements.autoRefresh.addEventListener("change", scheduleRefresh);
    elements.refreshInterval.addEventListener("change", scheduleRefresh);
}

async function handleLogin(event) {
    event.preventDefault();

    const username = elements.username.value.trim();
    const password = elements.password.value;
    if (!username || !password) {
        showLoginError("Enter username and password.");
        return;
    }

    elements.loginButton.disabled = true;
    showLoginError("");

    const authHeader = createBasicAuth(username, password);
    try {
        await apiRequest("/session", authHeader);
        state.authHeader = authHeader;
        state.username = username;
        writeSession(AUTH_KEY, authHeader);
        writeSession(USER_KEY, username);
        showApp();
        refreshAll();
        scheduleRefresh();
    } catch (error) {
        showLoginError(error.status === 401 ? "Invalid credentials." : error.message);
    } finally {
        elements.loginButton.disabled = false;
    }
}

function logout() {
    clearTimeout(state.refreshTimer);
    state.refreshTimer = null;
    state.authHeader = "";
    state.username = "";
    state.groups = [];
    state.selectedKey = null;
    removeSession(AUTH_KEY);
    removeSession(USER_KEY);
    elements.password.value = "";
    showLogin();
}

async function refreshAll() {
    if (!state.authHeader) {
        return;
    }

    try {
        const [health, groupsResponse, grafanaGroups, grafanaShards, prometheusText] = await Promise.all([
            apiRequest("/health"),
            apiRequest("/groups"),
            grafanaRequest("/groups"),
            grafanaRequest("/shards?streamPrefix=&consumerGroup="),
            fetchPrometheusMetrics(),
        ]);

        state.groups = groupsResponse.groups || [];
        state.grafanaGroups = Array.isArray(grafanaGroups) ? grafanaGroups : [];
        state.grafanaShards = Array.isArray(grafanaShards) ? grafanaShards : [];
        if (!state.selectedKey && state.groups.length > 0) {
            state.selectedKey = groupKey(state.groups[0]);
        }
        if (state.selectedKey && !state.groups.some((group) => groupKey(group) === state.selectedKey)) {
            state.selectedKey = state.groups.length > 0 ? groupKey(state.groups[0]) : null;
        }

        renderHealth(health);
        renderConsoleOverview(health, prometheusText);
        renderGroups();
        renderLastUpdated();
    } catch (error) {
        handleApiError(error);
    }
}

async function refreshSelectedGroup() {
    if (!state.selectedKey) {
        showEmptyState();
        return;
    }

    const group = selectedGroup();
    if (!group) {
        showEmptyState();
        return;
    }

    const base = groupPath(group);
    try {
        const [detail, members, assignments, consumption, offsets, migrations] = await Promise.all([
            apiRequest(base),
            apiRequest(`${base}/members`),
            apiRequest(`${base}/assignments`),
            apiRequest(`${base}/consumption`),
            apiRequest(`${base}/offsets`),
            apiRequest(`${base}/migrations`),
        ]);

        const memberList = members.members || [];
        const progress = consumption.progress || [];
        renderGroupDetail(detail);
        renderSignals(detail, memberList, assignments, progress, offsets);
        renderMembers(memberList);
        renderAssignments(assignments, detail, offsets);
        renderMessageShardOptions(offsets.shards || []);
        renderMemberWorkload(memberList, assignments, progress);
        renderConsumption(progress);
        renderMigrations(migrations);
        renderLastUpdated();
        if (!state.messageCursor) {
            await loadMessages(true);
        }
    } catch (error) {
        handleApiError(error);
    }
}

async function apiRequest(path, authOverride) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    let response;

    try {
        response = await fetch(`${API_BASE}${path}`, {
            headers: {
                "Accept": "application/json",
                "Authorization": authOverride || state.authHeader,
            },
            cache: "no-store",
            signal: controller.signal,
        });
    } catch (error) {
        if (error.name === "AbortError") {
            throw new Error("Request timed out. Check coordinator or Redis connectivity.");
        }
        throw error;
    } finally {
        clearTimeout(timeout);
    }

    if (response.status === 401) {
        const error = new Error("Unauthorized");
        error.status = 401;
        throw error;
    }
    if (response.status === 403) {
        const error = new Error("This account does not have monitor access.");
        error.status = 403;
        throw error;
    }
    if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with HTTP ${response.status}`);
    }
    return response.json();
}

async function grafanaRequest(path) {
    return authenticatedFetchJson(`${GRAFANA_BASE}${path}`);
}

async function authenticatedFetchJson(url) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    let response;

    try {
        response = await fetch(url, {
            headers: {
                "Accept": "application/json",
                "Authorization": state.authHeader,
            },
            cache: "no-store",
            signal: controller.signal,
        });
    } catch (error) {
        if (error.name === "AbortError") {
            throw new Error("Request timed out. Check coordinator or Redis connectivity.");
        }
        throw error;
    } finally {
        clearTimeout(timeout);
    }

    if (response.status === 401) {
        const error = new Error("Unauthorized");
        error.status = 401;
        throw error;
    }
    if (response.status === 403) {
        const error = new Error("This account does not have monitor access.");
        error.status = 403;
        throw error;
    }
    if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with HTTP ${response.status}`);
    }
    return response.json();
}

async function fetchPrometheusMetrics() {
    try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
        const response = await fetch("/actuator/prometheus", {
            headers: { "Accept": "text/plain" },
            cache: "no-store",
            signal: controller.signal,
        });
        clearTimeout(timeout);
        return response.ok ? response.text() : "";
    } catch (_error) {
        return "";
    }
}

function renderHealth(health) {
    elements.coordinatorId.textContent = health.coordinatorId || "Unknown coordinator";
    elements.redisStatus.textContent = health.redis || "-";
    elements.loopStatus.textContent = health.loop || "-";
    setBadge(elements.healthBadge, health.status || "Unknown", health.status === "UP" ? "ok" : "bad");
}

function renderConsoleOverview(health, prometheusText) {
    elements.consoleOverview.classList.remove("hidden");
    const shards = state.grafanaShards;
    const groups = state.grafanaGroups;
    const activeMembers = groups.reduce((sum, group) => sum + Number(group.activeMembers || 0), 0);
    const totalMembers = groups.reduce((sum, group) => sum + Number(group.totalMembers || 0), 0);
    const streamEntries = sumByUniqueShard(shards, "streamLength");
    const totalMemory = sumByUniqueShard(shards, "memoryUsageBytes");
    const totalLag = groups.reduce((sum, group) => sum + Number(group.totalLag || 0), 0);
    const pending = groups.reduce((sum, group) => sum + Number(group.totalPendingCount || 0), 0);

    elements.globalCoordinatorSignal.textContent = health.status === "UP" ? "Online" : valueOrDash(health.status);
    elements.globalActiveConsumers.textContent = `${compactNumber(activeMembers)} / ${compactNumber(totalMembers)}`;
    elements.globalStreamEntries.textContent = compactNumber(streamEntries);
    elements.globalStreamMemory.textContent = formatBytes(totalMemory);
    elements.globalTotalLag.textContent = compactNumber(totalLag);
    elements.globalPendingEntries.textContent = compactNumber(pending);
    elements.overviewShardCount.textContent = `${shards.length} rows`;
    elements.grafanaGroupCount.textContent = `${groups.length} groups`;

    renderStreamOverview(shards, groups);
    renderGrafanaGroupsTable(groups);
    renderGlobalMessageSelectors();
    renderApiMetrics(prometheusText || "");
    renderGrafanaLinks();
    if (shouldLoadGlobalMessages()) {
        loadGlobalMessages();
    }
}

function renderStreamOverview(shards, groups) {
    if (!Array.isArray(shards) || shards.length === 0) {
        elements.streamOverview.innerHTML = `<p class="empty-line">No stream shard rows returned.</p>`;
        return;
    }

    const groupsByStream = new Map();
    groups.forEach((group) => {
        const stream = group.streamPrefix || "-";
        const list = groupsByStream.get(stream) || [];
        list.push(group);
        groupsByStream.set(stream, list);
    });
    const shardsByGroup = new Map();
    shards.forEach((shard) => {
        const key = groupKey(shard);
        const list = shardsByGroup.get(key) || [];
        list.push(shard);
        shardsByGroup.set(key, list);
    });

    elements.streamOverview.innerHTML = Array.from(groupsByStream.entries())
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([streamPrefix, streamGroups]) => {
            const streamLag = streamGroups.reduce((sum, group) => sum + Number(group.totalLag || 0), 0);
            const streamEntries = Math.max(...streamGroups.map((group) => Number(group.totalStreamLength || 0)), 0);
            return `
                <section class="stream-card">
                    <div class="stream-card-head">
                        <div>
                            <span class="health-dot ${streamLag > 0 ? "warn" : "ok"}"></span>
                            <strong>${escapeHtml(streamPrefix)}</strong>
                            <small>${streamGroups.length} consumer group${streamGroups.length === 1 ? "" : "s"}</small>
                        </div>
                        <div class="stream-card-stats">
                            <span>lag ${compactNumber(streamLag)}</span>
                            <span>entries ${compactNumber(streamEntries)}</span>
                        </div>
                    </div>
                    <div class="stream-card-groups">
                        ${streamGroups.sort((a, b) => a.consumerGroup.localeCompare(b.consumerGroup)).map((group) => {
                            const rows = (shardsByGroup.get(groupKey(group)) || []).sort((a, b) => Number(a.shardIndex) - Number(b.shardIndex));
                            return `
                                <section class="stream-group-card">
                                    <div class="stream-group-head">
                                        <strong>${escapeHtml(group.consumerGroup)}</strong>
                                        <span>${escapeHtml(group.state)} · ${escapeHtml(group.assignedShardRatio || `${group.currentShards}/${group.shardCount}`)} assigned</span>
                                    </div>
                                    <div class="mini-shard-grid">
                                        ${rows.map((shard) => miniShard(shard)).join("") || `<p class="empty-line">No shards.</p>`}
                                    </div>
                                </section>
                            `;
                        }).join("")}
                    </div>
                </section>
            `;
        }).join("");
}

function miniShard(shard) {
    const lag = numberOrNull(shard.lag);
    const tone = lag === null ? "unknown" : lag === 0 ? "ok" : lag < 100 ? "warn" : "bad";
    const owner = shard.currentOwnerMemberIds || shard.targetOwnerMemberIds || "-";
    const detail = [
        `:${shard.shardIndex}`,
        `lag ${valueOrDash(shard.lag)}`,
        `length ${compactNumber(shard.streamLength)}`,
        `pending ${compactNumber(shard.pendingCount)}`,
        `owner ${owner}`,
        `redis ${shard.redisNodeEndpoint || "-"}`,
        `produced/s ${valueOrDash(formatRate(shard.producedPerSecond))}`,
        `consumed/s ${valueOrDash(formatRate(shard.consumedPerSecond))}`,
        `memory ${formatBytes(shard.memoryUsageBytes)}`,
    ].join(" / ");
    return `<span class="mini-shard ${tone}" title="${escapeAttr(detail)}" aria-label="${escapeAttr(detail)}">:${escapeHtml(shard.shardIndex)}</span>`;
}

function renderGrafanaGroupsTable(groups) {
    if (!groups.length) {
        elements.grafanaGroupsTable.innerHTML = `<tr><td colspan="9" class="empty-line">No coordinator groups.</td></tr>`;
        return;
    }
    elements.grafanaGroupsTable.innerHTML = groups
        .slice()
        .sort((a, b) => a.streamPrefix.localeCompare(b.streamPrefix) || a.consumerGroup.localeCompare(b.consumerGroup))
        .map((group) => `
            <tr>
                <td><strong>${escapeHtml(group.streamPrefix)}</strong></td>
                <td>${escapeHtml(group.consumerGroup)}<br>${statePill(group.state)}</td>
                <td>${escapeHtml(group.assignedShardRatio || `${group.currentShards} / ${group.shardCount}`)}</td>
                <td>${compactNumber(group.activeMembers)} / ${compactNumber(group.totalMembers)}</td>
                <td>${valueOrDash(group.totalLag)}</td>
                <td>${compactNumber(group.totalPendingCount)}</td>
                <td>${formatRate(group.producedPerSecond)}</td>
                <td>${formatRate(group.consumedPerSecond)}</td>
                <td>${formatBytes(group.totalMemoryUsageBytes)}</td>
            </tr>
        `).join("");
}

function renderGlobalMessageSelectors() {
    const streams = unique(state.grafanaGroups.map((group) => group.streamPrefix)).sort();
    if (!state.globalMessages.streamPrefix || !streams.includes(state.globalMessages.streamPrefix)) {
        state.globalMessages.streamPrefix = streams[0] || "";
    }
    const groups = state.grafanaGroups
        .filter((group) => group.streamPrefix === state.globalMessages.streamPrefix)
        .map((group) => group.consumerGroup)
        .sort();
    if (!state.globalMessages.consumerGroup || !groups.includes(state.globalMessages.consumerGroup)) {
        state.globalMessages.consumerGroup = groups[0] || "";
    }
    const shardRows = state.grafanaShards
        .filter((shard) => shard.streamPrefix === state.globalMessages.streamPrefix && shard.consumerGroup === state.globalMessages.consumerGroup)
        .sort((a, b) => Number(a.shardIndex) - Number(b.shardIndex));
    const shardValues = shardRows.map((shard) => String(shard.shardIndex));
    if (state.globalMessages.shardIndex !== "all" && !shardValues.includes(state.globalMessages.shardIndex)) {
        state.globalMessages.shardIndex = "all";
    }

    elements.globalMessageStream.innerHTML = streams.map((stream) => {
        const selected = stream === state.globalMessages.streamPrefix ? " selected" : "";
        return `<option value="${escapeAttr(stream)}"${selected}>${escapeHtml(stream)}</option>`;
    }).join("") || `<option value="">No streams</option>`;
    elements.globalMessageGroup.innerHTML = groups.map((group) => {
        const selected = group === state.globalMessages.consumerGroup ? " selected" : "";
        return `<option value="${escapeAttr(group)}"${selected}>${escapeHtml(group)}</option>`;
    }).join("") || `<option value="">No groups</option>`;
    elements.globalMessageShard.innerHTML = `<option value="all"${state.globalMessages.shardIndex === "all" ? " selected" : ""}>All shards</option>` +
        shardRows.map((shard) => {
            const selected = String(shard.shardIndex) === state.globalMessages.shardIndex ? " selected" : "";
            return `<option value="${escapeAttr(shard.shardIndex)}"${selected}>:${escapeHtml(shard.shardIndex)} / lag ${escapeHtml(valueOrDash(shard.lag))}</option>`;
        }).join("");
    updateGlobalMessageButtons();
}

async function loadGlobalMessages() {
    if (!shouldLoadGlobalMessages()) {
        elements.globalMessageRows.innerHTML = `<tr><td colspan="4" class="empty-line">Select a stream and consumer group.</td></tr>`;
        return;
    }
    const params = new URLSearchParams({
        streamPrefix: state.globalMessages.streamPrefix,
        consumerGroup: state.globalMessages.consumerGroup,
        shardIndex: state.globalMessages.shardIndex || "all",
        direction: "BACKWARD",
        limit: elements.globalMessageLimit.value || "25",
    });
    const cursor = state.globalMessages.cursors[state.globalMessages.page];
    if (cursor) {
        params.set("cursor", cursor);
    }
    elements.globalMessageStatus.textContent = "Loading messages...";
    updateGlobalMessageButtons(true);
    try {
        const rows = await grafanaRequest(`/messages?${params}`);
        const limit = Number(elements.globalMessageLimit.value || 25);
        const total = rows.length > 0 ? Number(rows[0].pageTotalMessages || 0) : 0;
        state.globalMessages.totalMessages = total;
        state.globalMessages.nextCursor = rows.length > 0 ? rows[rows.length - 1].pageNextCursor || "" : "";
        elements.globalMessageRows.innerHTML = rows.map(grafanaMessageRow).join("") ||
            `<tr><td colspan="4" class="empty-line">No records.</td></tr>`;
        const pageCount = total > 0 ? Math.max(1, Math.ceil(total / limit)) : 1;
        elements.globalMessageStatus.textContent = `${rows.length}/${limit} message(s), page ${Math.min(state.globalMessages.page + 1, pageCount)} / ${pageCount}, total ${compactNumber(total)}`;
    } catch (error) {
        elements.globalMessageRows.innerHTML = `<tr><td colspan="4" class="empty-line">${escapeHtml(error.message || "Failed to load messages.")}</td></tr>`;
        elements.globalMessageStatus.textContent = "Failed to load messages.";
    } finally {
        updateGlobalMessageButtons(false);
    }
}

function grafanaMessageRow(row) {
    return `
        <tr>
            <td><span class="mono">${escapeHtml(formatInstant(row.recordTime || row.recordTimestampMs))}</span></td>
            <td><span class="mono">${escapeHtml(row.shardLabel || `:${row.shardIndex}`)}</span></td>
            <td><span class="mono">${escapeHtml(row.recordId)}</span></td>
            <td><div class="field-list"><span class="field-pill">${escapeHtml(row.fieldsJson || row.payload || "{}")}</span></div></td>
        </tr>
    `;
}

function renderApiMetrics(prometheusText) {
    const metrics = parseApiMetrics(prometheusText);
    if (!metrics.length) {
        setBadge(elements.apiMetricStatus, "No scrape", "warn");
        elements.apiMetricList.innerHTML = `<p class="empty-line">No API metrics are available yet.</p>`;
        return;
    }
    setBadge(elements.apiMetricStatus, `${metrics.length} routes`, "ok");
    elements.apiMetricList.innerHTML = metrics
        .sort((a, b) => b.count - a.count)
        .slice(0, 12)
        .map((metric) => `
            <div class="api-metric-row">
                <span class="mono">${escapeHtml(metric.route)}</span>
                <strong>${compactNumber(metric.count)} req</strong>
                <small>${escapeHtml(metric.method)} ${escapeHtml(metric.status)} · avg ${formatDuration(metric.avgSeconds)}</small>
            </div>
        `).join("");
}

function parseApiMetrics(text) {
    const byKey = new Map();
    text.split("\n").forEach((line) => {
        const count = line.match(/^redis_stream_coord_api_request_duration_seconds_count\{([^}]*)}\s+([0-9.eE+-]+)/);
        const sum = line.match(/^redis_stream_coord_api_request_duration_seconds_sum\{([^}]*)}\s+([0-9.eE+-]+)/);
        if (!count && !sum) {
            return;
        }
        const labels = parsePrometheusLabels((count || sum)[1]);
        const key = [labels.method || "-", labels.route || "-", labels.status || "-", labels.stream || "-", labels.group || "-"].join("\u0000");
        const entry = byKey.get(key) || {
            method: labels.method || "-",
            route: labels.route || "-",
            status: labels.status || "-",
            stream: labels.stream || "-",
            group: labels.group || "-",
            count: 0,
            sum: 0,
        };
        if (count) entry.count = Number(count[2] || 0);
        if (sum) entry.sum = Number(sum[2] || 0);
        byKey.set(key, entry);
    });
    return Array.from(byKey.values()).map((entry) => ({
        ...entry,
        avgSeconds: entry.count > 0 ? entry.sum / entry.count : 0,
    }));
}

function parsePrometheusLabels(source) {
    const labels = {};
    source.replace(/([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\"|[^"])*)"/g, (_match, key, value) => {
        labels[key] = value.replaceAll('\\"', '"');
        return "";
    });
    return labels;
}

function renderGrafanaLinks() {
    const origin = "https://monitor.ghkdqhrbals.org";
    const stream = encodeURIComponent(state.globalMessages.streamPrefix || "create-order");
    const group = encodeURIComponent(state.globalMessages.consumerGroup || "demo-workers");
    elements.grafanaOverviewLink.href = `${origin}/d/redis-stream-coordinator/redis-stream-coordinator-overview?orgId=1&from=now-15m&to=now&timezone=browser&refresh=30s`;
    elements.grafanaDetailLink.href = `${origin}/d/redis-stream-coordinator-stream-detail/redis-stream-coordinator-stream-detail?orgId=1&from=now-15m&to=now&timezone=browser&var-streamPrefix=${stream}&var-consumerGroup=${group}&var-shardIndex=all&refresh=30s`;
    elements.grafanaApiLink.href = `${origin}/d/redis-stream-coordinator-api/redis-stream-coordinator-api-performance?orgId=1&from=now-15m&to=now&timezone=browser&var-streamPrefix=${stream}&var-consumerGroup=${group}&var-apiRoute=$__all&refresh=30s`;
}

function renderGroups() {
    if (state.groups.length === 0) {
        elements.groupList.innerHTML = `<p class="empty-line">No streams.</p>`;
        return;
    }

    const streams = new Map();
    state.groups.forEach((group) => {
        const list = streams.get(group.streamPrefix) || [];
        list.push(group);
        streams.set(group.streamPrefix, list);
    });

    elements.groupList.innerHTML = Array.from(streams.entries())
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([streamPrefix, groups]) => {
            const shardCount = Math.max(...groups.map((group) => Number(group.shardCount || 0)), 0);
            const totalLag = groups.reduce((sum, group) => sum + Number(group.totalLag || 0), 0);
            return `
                <section class="stream-nav-section">
                    <div class="stream-nav-head">
                        <strong>${escapeHtml(streamPrefix)}</strong>
                        <span>${groups.length} group${groups.length === 1 ? "" : "s"} · ${shardCount} shards · lag ${compactNumber(totalLag)}</span>
                    </div>
                    <div class="stream-nav-groups">
                        ${groups.sort((a, b) => a.consumerGroup.localeCompare(b.consumerGroup)).map((group) => {
                            const key = groupKey(group);
                            const selected = key === state.selectedKey ? " selected" : "";
                            return `
                                <button class="group-row${selected}" type="button" data-key="${escapeAttr(key)}">
                                    <strong>${escapeHtml(group.consumerGroup)}</strong>
                                    <span>${escapeHtml(group.state)} · ${escapeHtml(group.assignedShardRatio || `${group.currentShards || 0}/${group.shardCount || 0}`)} assigned</span>
                                </button>
                            `;
                        }).join("")}
                    </div>
                </section>
            `;
        }).join("");

    elements.groupList.querySelectorAll(".group-row").forEach((button) => {
        button.addEventListener("click", () => {
            state.selectedKey = button.dataset.key;
            const group = selectedGroup();
            if (group) {
                state.globalMessages.streamPrefix = group.streamPrefix;
                state.globalMessages.consumerGroup = group.consumerGroup;
                state.globalMessages.shardIndex = "all";
                resetGlobalMessages();
                renderGlobalMessageSelectors();
                loadGlobalMessages();
                renderGrafanaLinks();
            }
            renderGroups();
        });
    });
}

function renderGroupDetail(group) {
    elements.emptyState.classList.add("hidden");
    elements.detailView.classList.remove("hidden");
    elements.pageTitle.textContent = `${group.streamPrefix} / ${group.consumerGroup}`;
    elements.pageSubtitle.textContent = `stream key pattern: ${group.streamPrefix}:{index}`;
    elements.groupState.textContent = group.state || "-";
    elements.metadataVersion.textContent = valueOrDash(group.metadataVersion);
    elements.groupEpoch.textContent = valueOrDash(group.groupEpoch);
    elements.assignmentEpoch.textContent = valueOrDash(group.assignmentEpoch);
    elements.shardCount.textContent = valueOrDash(group.shardCount);
}

function renderSignals(group, members, assignments, progress, offsets) {
    const activeMembers = members.filter((member) => member.state === "ACTIVE" || member.state === "STARTING").length;
    const currentTotal = sumAssignmentSize(assignments.currentAssignments);
    const targetTotal = sumAssignmentSize(assignments.targetAssignment);
    const revokingTotal = sumAssignmentSize(assignments.revokeProgress);
    const pendingTotal = Number(offsets?.totalPendingCount ?? progress.reduce((sum, item) => sum + Number(item.pendingCount || 0), 0));
    const latestHeartbeat = latestInstant(members.map((member) => member.lastHeartbeatAt));

    elements.memberSignal.textContent = `${activeMembers}/${members.length}`;
    elements.memberSignalHint.textContent = "active / total";
    elements.shardSignal.textContent = `${currentTotal}/${targetTotal || group.shardCount || 0}`;
    elements.shardSignalHint.textContent = "current / target";
    elements.revokingSignal.textContent = String(revokingTotal);
    elements.pendingSignal.textContent = compactNumber(offsets?.totalLag ?? pendingTotal);
    elements.pendingSignal.nextElementSibling.textContent = offsets?.totalLag === null ? "pending records" : "total lag";
    elements.heartbeatSignal.textContent = latestHeartbeat ? relativeAge(latestHeartbeat) : "-";
    elements.heartbeatSignalHint.textContent = latestHeartbeat ? formatTime(latestHeartbeat.toISOString()) : "latest member report";
}

function renderMembers(members) {
    elements.memberCount.textContent = String(members.length);
    if (members.length === 0) {
        elements.membersTable.innerHTML = `<tr><td colspan="8" class="empty-line">No active members.</td></tr>`;
        return;
    }

    elements.membersTable.innerHTML = members.map((member) => `
        <tr>
            <td><span class="mono">${escapeHtml(member.memberId)}</span><br>${escapeHtml(member.memberName || "")}</td>
            <td>${statePill(member.state)}</td>
            <td>${valueOrDash(member.memberEpoch)}</td>
            <td>${valueOrDash(member.metadataVersion)}</td>
            <td>${formatShardCount(member.currentAssignment)}</td>
            <td>${formatShardCount(member.revoking)}</td>
            <td>${valueOrDash(member.activeConsumerWorkers)} / ${valueOrDash(member.runtimeMaxConcurrency)}</td>
            <td>${formatTime(member.lastHeartbeatAt)}</td>
        </tr>
    `).join("");
}

function renderAssignments(assignments, group, offsets) {
    const violations = assignments.invariantViolations || [];
    setBadge(
        elements.assignmentStatus,
        violations.length === 0 ? "Healthy" : `${violations.length} violation`,
        violations.length === 0 ? "ok" : "bad",
    );

    const target = assignments.targetAssignment || {};
    const current = assignments.currentAssignments || {};
    const revoking = assignments.revokeProgress || {};
    const memberIds = unique([...Object.keys(target), ...Object.keys(current), ...Object.keys(revoking)]).sort();

    renderShardMatrix(group, target, current, revoking, violations, offsets);

    const rows = memberIds.map((memberId) => `
        <div class="assignment-row">
            <div class="row-label mono">${escapeHtml(memberId)}</div>
            <div class="row-value"><strong>Target</strong>${formatShardChips(target[memberId], group.streamPrefix)}</div>
            <div class="row-value"><strong>Current</strong>${formatShardChips(current[memberId], group.streamPrefix)}</div>
            <div class="row-value"><strong>Revoking</strong>${formatShardChips(revoking[memberId], group.streamPrefix)}</div>
        </div>
    `);

    if (violations.length > 0) {
        rows.push(`
            <div class="assignment-row">
                <div class="row-label">Invariant</div>
                <div class="row-value" style="grid-column: span 3">${escapeHtml(violations.join("; "))}</div>
            </div>
        `);
    }

    elements.assignmentGrid.innerHTML = rows.length > 0 ? rows.join("") : `<p class="empty-line">No assignments.</p>`;
}

function renderShardMatrix(group, target, current, revoking, violations, offsets) {
    const targetOwners = ownersByShard(target);
    const currentOwners = ownersByShard(current);
    const revokingOwners = ownersByShard(revoking);
    const shards = allKnownShards(group, targetOwners, currentOwners, revokingOwners);

    if (shards.length === 0) {
        elements.shardMatrix.innerHTML = `<p class="empty-line">No shard ownership has been reported.</p>`;
        return;
    }

    const offsetByShard = new Map((offsets?.shards || []).map((offset) => [shardIdentity(offset.shard), offset]));
    const rows = shards.map((shard) => {
        const key = shardIdentity(shard);
        const offset = offsetByShard.get(key);
        const targetList = targetOwners.get(key) || [];
        const currentList = currentOwners.get(key) || [];
        const revokeList = revokingOwners.get(key) || [];
        const phase = shardPhase(targetList, currentList, revokeList);
        return `
            <tr>
                <td>
                    <span class="shard-name">${escapeHtml(formatShardKey(group.streamPrefix, shard))}</span>
                    <span class="shard-meta">${escapeHtml(formatShardLabel(shard))}</span>
                </td>
                <td>${formatOwnerList(targetList)}</td>
                <td>${formatOwnerList(currentList)}</td>
                <td>${formatOwnerList(revokeList)}</td>
                <td>
                    ${phasePill(phase)}
                    <span class="offset-line">end ${escapeHtml(offset?.lastRecordId || offset?.lastGeneratedId || "-")}</span>
                    <span class="offset-line">consumer ${escapeHtml(offset?.consumerLastAckedId || offset?.consumerLastDeliveredId || offset?.groupLastDeliveredId || "-")}</span>
                    <span class="offset-line">lag ${escapeHtml(valueOrDash(offset?.lag))} / pending ${escapeHtml(valueOrDash(offset?.pendingCount))}</span>
                </td>
            </tr>
        `;
    }).join("");

    const invariantRows = violations.length > 0
        ? `<div class="invariant-box">${escapeHtml(violations.join("; "))}</div>`
        : "";

    elements.shardMatrix.innerHTML = `
        ${invariantRows}
        <div class="table-wrap matrix-wrap">
            <table>
                <thead>
                <tr>
                    <th>Shard key</th>
                    <th>Target owner</th>
                    <th>Current owner</th>
                    <th>Revoking owner</th>
                    <th>Phase</th>
                </tr>
                </thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
    `;
}

function renderMessageShardOptions(offsets) {
    if (!Array.isArray(offsets) || offsets.length === 0) {
        elements.messageShardSelect.innerHTML = "";
        elements.messageBrowser.innerHTML = `<p class="empty-line">No stream shards are available.</p>`;
        return;
    }
    const currentExists = state.selectedMessageShard && offsets.some((offset) => shardIdentity(offset.shard) === state.selectedMessageShard);
    if (!currentExists) {
        state.selectedMessageShard = shardIdentity(offsets[0].shard);
        state.messageCursor = null;
    }
    elements.messageShardSelect.innerHTML = offsets.map((offset) => {
        const id = shardIdentity(offset.shard);
        const selected = id === state.selectedMessageShard ? " selected" : "";
        return `<option value="${escapeAttr(id)}"${selected}>${escapeHtml(offset.streamKey)} / lag ${escapeHtml(valueOrDash(offset.lag))}</option>`;
    }).join("");
}

async function loadMessages(reset) {
    const group = selectedGroup();
    if (!group || !state.selectedMessageShard) {
        return;
    }
    if (reset) {
        state.messageCursor = null;
    }
    const shardIndex = Number(state.selectedMessageShard);
    const base = groupPath(group);
    const params = new URLSearchParams({
        direction: "BACKWARD",
        limit: "25",
    });
    if (state.messageCursor) {
        params.set("cursor", state.messageCursor);
    }
    try {
        const page = await apiRequest(`${base}/shards/${shardIndex}/messages?${params}`);
        state.messageCursor = page.nextCursor;
        renderMessages(page, reset);
    } catch (error) {
        elements.messageBrowser.innerHTML = `<p class="empty-line">${escapeHtml(error.message || "Failed to load messages.")}</p>`;
    }
}

function renderMessages(page, reset) {
    const rows = page.records.map((record) => messageRow(record)).join("");
    const existingRows = !reset
        ? elements.messageBrowser.querySelector("tbody")?.innerHTML || ""
        : "";
    elements.messageBrowser.innerHTML = `
        <div class="message-browser-header">
            <div>
                <span class="shard-name">${escapeHtml(page.streamKey)}</span>
                <span class="shard-meta">latest records, ${escapeHtml(page.direction.toLowerCase())}, limit ${page.limit}</span>
            </div>
            <button id="messageOlderButton" class="ghost-button small" type="button" ${page.nextCursor ? "" : "disabled"}>Load older</button>
        </div>
        <div class="table-wrap">
            <table>
                <thead>
                <tr>
                    <th>Time</th>
                    <th>Shard</th>
                    <th>Offset</th>
                    <th>Fields</th>
                </tr>
                </thead>
                <tbody>${existingRows}${rows || `<tr><td colspan="4" class="empty-line">No records.</td></tr>`}</tbody>
            </table>
        </div>
    `;
    const button = document.getElementById("messageOlderButton");
    if (button) {
        button.addEventListener("click", () => loadMessages(false));
    }
}

function messageRow(record) {
    const fields = Object.entries(record.fields || {})
        .map(([key, value]) => `<span class="field-pill"><b>${escapeHtml(key)}</b>${escapeHtml(value)}</span>`)
        .join("");
    return `
        <tr>
            <td>${formatRecordTime(record)}</td>
            <td><span class="mono">:${escapeHtml(record.shard?.shardIndex ?? "-")}</span></td>
            <td><span class="mono">${escapeHtml(record.recordId)}</span></td>
            <td><div class="field-list">${fields || `<span class="muted-text">empty</span>`}</div></td>
        </tr>
    `;
}

function formatRecordTime(record) {
    const value = record.recordTime || record.recordTimestampMs;
    if (!value) {
        return "-";
    }
    const date = typeof value === "number" ? new Date(value) : new Date(value);
    if (Number.isNaN(date.getTime())) {
        return escapeHtml(String(value));
    }
    return `<span class="mono">${escapeHtml(date.toLocaleString())}</span>`;
}

function renderMemberWorkload(members, assignments, progress) {
    elements.workloadCount.textContent = String(members.length);
    if (members.length === 0) {
        elements.memberWorkload.innerHTML = `<p class="empty-line">No workload to display.</p>`;
        return;
    }

    const target = assignments.targetAssignment || {};
    const current = assignments.currentAssignments || {};
    const revoking = assignments.revokeProgress || {};
    const progressByMember = progress.reduce((acc, item) => {
        const currentItem = acc.get(item.memberId) || { shards: 0, pending: 0 };
        currentItem.shards += 1;
        currentItem.pending += Number(item.pendingCount || 0);
        acc.set(item.memberId, currentItem);
        return acc;
    }, new Map());

    elements.memberWorkload.innerHTML = members.map((member) => {
        const max = Number(member.runtimeMaxConcurrency || 0);
        const active = Number(member.activeConsumerWorkers || 0);
        const capacityPct = max > 0 ? Math.min(100, Math.round((active / max) * 100)) : 0;
        const memberProgress = progressByMember.get(member.memberId) || { shards: 0, pending: 0 };
        return `
            <div class="workload-row">
                <div>
                    <span class="member-name mono">${escapeHtml(member.memberId)}</span>
                    <span class="member-subtitle">${escapeHtml(member.memberName || member.consumerGroup || "")}</span>
                </div>
                <div class="workload-meter" aria-label="worker capacity ${active} of ${max}">
                    <span style="width: ${capacityPct}%"></span>
                </div>
                <div class="workload-stats">
                    <span>${statePill(member.state)}</span>
                    <span>${active}/${max || "-"} workers</span>
                    <span>${formatShardCount(current[member.memberId])} current</span>
                    <span>${formatShardCount(target[member.memberId])} target</span>
                    <span>${formatShardCount(revoking[member.memberId])} revoking</span>
                    <span>${compactNumber(memberProgress.pending)} pending</span>
                </div>
            </div>
        `;
    }).join("");
}

function renderConsumption(progress) {
    elements.progressCount.textContent = String(progress.length);
    if (progress.length === 0) {
        elements.progressList.innerHTML = `<p class="empty-line">No consumption progress.</p>`;
        return;
    }

    elements.progressList.innerHTML = `
        <div class="table-wrap">
            <table>
                <thead>
                <tr>
                    <th>Member</th>
                    <th>Shard key</th>
                    <th>Delivered</th>
                    <th>Acked</th>
                    <th>Pending</th>
                    <th>Updated</th>
                </tr>
                </thead>
                <tbody>
                ${progress.map((item) => `
                    <tr>
                        <td><span class="mono">${escapeHtml(item.memberId)}</span></td>
                        <td>
                            <span class="shard-name">${escapeHtml(item.streamKey || formatShardKey(item.streamPrefix, item.shard))}</span>
                            <span class="shard-meta">${escapeHtml(formatShardLabel(item.shard))}</span>
                        </td>
                        <td><span class="mono">${escapeHtml(item.lastDeliveredId || "-")}</span></td>
                        <td><span class="mono">${escapeHtml(item.lastAckedId || "-")}</span></td>
                        <td>${compactNumber(item.pendingCount || 0)}</td>
                        <td>${formatTime(item.updatedAt)}</td>
                    </tr>
                `).join("")}
                </tbody>
            </table>
        </div>
    `;
}

function renderMigrations(response) {
    const migrations = response.migrations || [];
    setBadge(elements.migrationStatus, response.activeReshardingId ? "Active" : "Idle", response.activeReshardingId ? "warn" : "ok");

    if (migrations.length === 0) {
        elements.migrationList.innerHTML = `<p class="empty-line">No resharding history.</p>`;
        return;
    }

    elements.migrationList.innerHTML = migrations.map((migration) => `
        <div class="migration-row">
            <div class="row-label mono">${escapeHtml(migration.reshardingId)}</div>
            <div class="row-value">
                ${escapeHtml(migration.state)} - ${migration.fromShardCount} to ${migration.toShardCount} shards
            </div>
        </div>
    `).join("");
}

function scheduleRefresh() {
    clearTimeout(state.refreshTimer);
    state.refreshTimer = null;

    if (!elements.autoRefresh.checked || !state.authHeader) {
        return;
    }

    const intervalMs = Number(elements.refreshInterval.value || 5000);
    state.refreshTimer = setTimeout(async () => {
        await refreshAll();
        scheduleRefresh();
    }, intervalMs);
}

function showLogin() {
    elements.loginView.classList.remove("hidden");
    elements.appView.classList.add("hidden");
    elements.username.value = state.username || "";
    elements.username.focus();
    showLoginError("");
}

function showApp() {
    elements.loginView.classList.add("hidden");
    elements.appView.classList.remove("hidden");
}

function showEmptyState() {
    elements.emptyState.classList.remove("hidden");
    elements.detailView.classList.add("hidden");
    elements.pageTitle.textContent = "Select a group";
    elements.pageSubtitle.textContent = "No active stream group selected.";
}

function showLoginError(message) {
    elements.loginError.textContent = message;
}

function handleApiError(error) {
    if (error.status === 401) {
        logout();
        showLoginError("Session expired. Sign in again.");
        return;
    }
    renderErrorState(error.message || "Monitoring request failed.");
    console.error(error);
}

function renderErrorState(message) {
    setBadge(elements.healthBadge, "Error", "bad");
    elements.redisStatus.textContent = "UNREACHABLE";
    elements.loopStatus.textContent = "-";
    elements.groupList.innerHTML = `<p class="empty-line">${escapeHtml(message)}</p>`;
}

function renderLastUpdated() {
    elements.lastUpdated.textContent = `Updated ${new Date().toLocaleTimeString()}`;
}

function shouldLoadGlobalMessages() {
    return Boolean(state.globalMessages.streamPrefix && state.globalMessages.consumerGroup);
}

function resetGlobalMessages() {
    state.globalMessages.cursors = [""];
    state.globalMessages.page = 0;
    state.globalMessages.nextCursor = "";
    state.globalMessages.totalMessages = null;
}

function updateGlobalMessageButtons(loading = false) {
    const total = Number(state.globalMessages.totalMessages);
    const limit = Number(elements.globalMessageLimit?.value || 25);
    const lastPage = Number.isFinite(total) && total > 0 ? Math.max(0, Math.ceil(total / limit) - 1) : 0;
    elements.globalMessageFirst.disabled = loading || state.globalMessages.page === 0;
    elements.globalMessagePrev.disabled = loading || state.globalMessages.page === 0;
    elements.globalMessageNext.disabled = loading || !state.globalMessages.nextCursor;
    elements.globalMessageLast.disabled = loading || !shouldLoadGlobalMessages() || state.globalMessages.page >= lastPage;
}

function selectedGroup() {
    return state.groups.find((group) => groupKey(group) === state.selectedKey);
}

function groupKey(group) {
    return `${group.streamPrefix}::${group.consumerGroup}`;
}

function groupPath(group) {
    return `/streams/${encodeURIComponent(group.streamPrefix)}/groups/${encodeURIComponent(group.consumerGroup)}`;
}

function createBasicAuth(username, password) {
    const bytes = new TextEncoder().encode(`${username}:${password}`);
    let binary = "";
    bytes.forEach((byte) => {
        binary += String.fromCharCode(byte);
    });
    return `Basic ${btoa(binary)}`;
}

function setBadge(element, value, tone) {
    element.textContent = value;
    element.className = `status-badge ${tone}`;
}

function statePill(value) {
    const stateValue = value || "UNKNOWN";
    const tone = stateValue === "ACTIVE" || stateValue === "STABLE" ? "ok" :
        stateValue === "EXPIRED" || stateValue === "FENCED" ? "bad" : "warn";
    return `<span class="status-badge ${tone}">${escapeHtml(stateValue)}</span>`;
}

function formatShardCount(shards) {
    return `${Array.isArray(shards) ? shards.length : 0}`;
}

function formatShardChips(shards, streamPrefix) {
    if (!Array.isArray(shards) || shards.length === 0) {
        return `<div class="chip-line"><span class="chip">none</span></div>`;
    }
    return `<div class="chip-line">${shards.map((shard) => `
        <span class="chip shard-chip" title="${escapeAttr(formatShardKey(streamPrefix, shard))}">
            <span>${escapeHtml(formatShardLabel(shard))}</span>
            <small>${escapeHtml(formatShardKey(streamPrefix, shard))}</small>
        </span>
    `).join("")}</div>`;
}

function formatShardLabel(shard) {
    if (!shard) {
        return "-";
    }
    return valueOrDash(shard.shardIndex);
}

function formatShardKey(streamPrefix, shard) {
    if (!shard) {
        return "-";
    }
    return `${streamPrefix || "stream"}:${valueOrDash(shard.shardIndex)}`;
}

function formatTime(value) {
    if (!value) {
        return "-";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return escapeHtml(value);
    }
    return date.toLocaleString();
}

function valueOrDash(value) {
    return value === null || value === undefined || value === "" ? "-" : value;
}

function numberOrNull(value) {
    if (value === null || value === undefined || value === "") {
        return null;
    }
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function sumByUniqueShard(rows, field) {
    const seen = new Set();
    return rows.reduce((sum, row) => {
        const key = `${row.streamPrefix}:${row.shardIndex}`;
        if (seen.has(key)) {
            return sum;
        }
        seen.add(key);
        return sum + Number(row[field] || 0);
    }, 0);
}

function sumAssignmentSize(assignments) {
    return Object.values(assignments || {}).reduce((sum, shards) => sum + (Array.isArray(shards) ? shards.length : 0), 0);
}

function latestInstant(values) {
    return values
        .filter(Boolean)
        .map((value) => new Date(value))
        .filter((date) => !Number.isNaN(date.getTime()))
        .sort((a, b) => b.getTime() - a.getTime())[0] || null;
}

function relativeAge(date) {
    const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
    if (seconds < 60) {
        return `${seconds}s ago`;
    }
    const minutes = Math.round(seconds / 60);
    if (minutes < 60) {
        return `${minutes}m ago`;
    }
    return `${Math.round(minutes / 60)}h ago`;
}

function compactNumber(value) {
    const number = Number(value || 0);
    return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(number);
}

function formatBytes(value) {
    const number = numberOrNull(value);
    if (number === null) {
        return "-";
    }
    if (number >= 1073741824) {
        return `${trimDecimal(number / 1073741824)} GiB`;
    }
    if (number >= 1048576) {
        return `${trimDecimal(number / 1048576)} MiB`;
    }
    if (number >= 1024) {
        return `${trimDecimal(number / 1024)} KiB`;
    }
    return `${number} B`;
}

function formatRate(value) {
    const number = numberOrNull(value);
    return number === null ? "-" : `${trimDecimal(number)}/s`;
}

function formatDuration(seconds) {
    const number = numberOrNull(seconds) || 0;
    if (number >= 60) {
        return `${trimDecimal(number / 60)} min`;
    }
    if (number >= 1) {
        return `${trimDecimal(number)} s`;
    }
    return `${trimDecimal(number * 1000)} ms`;
}

function trimDecimal(value) {
    return Number(value).toFixed(1).replace(/\.0$/, "");
}

function formatInstant(value) {
    if (!value) {
        return "-";
    }
    const date = typeof value === "number" ? new Date(value) : new Date(value);
    return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString();
}

function ownersByShard(assignments) {
    const owners = new Map();
    Object.entries(assignments || {}).forEach(([memberId, shards]) => {
        if (!Array.isArray(shards)) {
            return;
        }
        shards.forEach((shard) => {
            const key = shardIdentity(shard);
            const entry = owners.get(key) || { shard, members: [] };
            entry.members.push(memberId);
            owners.set(key, entry);
        });
    });
    return new Map(Array.from(owners.entries()).map(([key, entry]) => [key, entry.members.sort()]));
}

function allKnownShards(group, ...ownerMaps) {
    const byKey = new Map();
    const shardCount = Number(group.shardCount || 0);
    for (let index = 0; index < shardCount; index += 1) {
        const shard = { shardIndex: index };
        byKey.set(shardIdentity(shard), shard);
    }
    ownerMaps.forEach((ownerMap) => {
        ownerMap.forEach((_owners, key) => {
            byKey.set(key, { shardIndex: Number(key) });
        });
    });
    return Array.from(byKey.values()).sort((a, b) => a.shardIndex - b.shardIndex);
}

function shardIdentity(shard) {
    return `${Number(shard?.shardIndex || 0)}`;
}

function formatOwnerList(owners) {
    if (!owners || owners.length === 0) {
        return `<span class="muted-text">none</span>`;
    }
    return owners.map((owner) => `<span class="owner-pill mono">${escapeHtml(owner)}</span>`).join("");
}

function shardPhase(targetOwners, currentOwners, revokingOwners) {
    if (revokingOwners.length > 0) {
        return "revoking";
    }
    if (targetOwners.length > 1 || currentOwners.length > 1) {
        return "conflict";
    }
    if (targetOwners.length === 0 && currentOwners.length === 0) {
        return "unassigned";
    }
    if (targetOwners.join(",") !== currentOwners.join(",")) {
        return "moving";
    }
    return "stable";
}

function phasePill(phase) {
    const tone = phase === "stable" ? "ok" : phase === "conflict" || phase === "unassigned" ? "bad" : "warn";
    return `<span class="status-badge ${tone}">${escapeHtml(phase.toUpperCase())}</span>`;
}

function unique(values) {
    return Array.from(new Set(values));
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function escapeAttr(value) {
    return escapeHtml(value);
}

function readSession(key) {
    try {
        return sessionStorage.getItem(key) || "";
    } catch (_error) {
        return "";
    }
}

function writeSession(key, value) {
    try {
        sessionStorage.setItem(key, value);
    } catch (_error) {
        // Session storage can be disabled by browser policy. The console still works for the active page.
    }
}

function removeSession(key) {
    try {
        sessionStorage.removeItem(key);
    } catch (_error) {
        // Ignore storage cleanup failures.
    }
}
