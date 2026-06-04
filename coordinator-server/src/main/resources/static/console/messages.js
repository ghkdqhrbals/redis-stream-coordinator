const MESSAGE_API_BASE = "/coord/v1/monitoring";
const MESSAGE_AUTH_KEY = "redisStreamCoordinator.console.auth";
const MESSAGE_USER_KEY = "redisStreamCoordinator.console.user";
const MESSAGE_REQUEST_TIMEOUT_MS = 5000;

const messageState = {
    authHeader: readSession(MESSAGE_AUTH_KEY),
    username: readSession(MESSAGE_USER_KEY),
    groups: [],
    offsets: [],
    selectedGroupKey: "",
    selectedShardId: "",
    cursor: null,
};

const messageElements = {};

document.addEventListener("DOMContentLoaded", () => {
    bindMessageElements();
    bindMessageEvents();
    if (messageState.authHeader) {
        showMessageApp();
        refreshMessageExplorer();
    } else {
        showMessageLogin();
    }
});

function bindMessageElements() {
    [
        "messageLoginView",
        "messageAppView",
        "messageLoginForm",
        "messageUsername",
        "messagePassword",
        "messageLoginButton",
        "messageLoginError",
        "messageExplorerSubtitle",
        "messageExplorerRefresh",
        "messageExplorerSignOut",
        "messageGroupSelect",
        "messageShardPicker",
        "messageDirection",
        "messageLimit",
        "messageStreamLength",
        "messageLag",
        "messagePending",
        "messageEndOffset",
        "messageConsumerOffset",
        "messageStreamKey",
        "messagePageHint",
        "messageResetCursor",
        "messageNextPage",
        "messageError",
        "messageRows",
    ].forEach((id) => {
        messageElements[id] = document.getElementById(id);
    });
}

function bindMessageEvents() {
    messageElements.messageLoginForm.addEventListener("submit", handleMessageLogin);
    messageElements.messageExplorerSignOut.addEventListener("click", messageLogout);
    messageElements.messageExplorerRefresh.addEventListener("click", () => refreshMessageExplorer());
    messageElements.messageGroupSelect.addEventListener("change", () => {
        messageState.selectedGroupKey = messageElements.messageGroupSelect.value;
        messageState.selectedShardId = "";
        messageState.cursor = null;
        refreshSelectedMessageGroup();
    });
    messageElements.messageShardPicker.addEventListener("change", () => {
        messageState.selectedShardId = messageElements.messageShardPicker.value;
        messageState.cursor = null;
        renderSelectedOffset();
        loadMessagePage(true);
    });
    messageElements.messageDirection.addEventListener("change", () => {
        messageState.cursor = null;
        loadMessagePage(true);
    });
    messageElements.messageLimit.addEventListener("change", () => {
        messageState.cursor = null;
        loadMessagePage(true);
    });
    messageElements.messageResetCursor.addEventListener("click", () => {
        messageState.cursor = null;
        loadMessagePage(true);
    });
    messageElements.messageNextPage.addEventListener("click", () => loadMessagePage(false));
}

async function handleMessageLogin(event) {
    event.preventDefault();
    const username = messageElements.messageUsername.value.trim();
    const password = messageElements.messagePassword.value;
    if (!username || !password) {
        messageElements.messageLoginError.textContent = "Enter username and password.";
        return;
    }

    messageElements.messageLoginButton.disabled = true;
    messageElements.messageLoginError.textContent = "";
    const authHeader = createBasicAuth(username, password);
    try {
        await messageApiRequest("/session", authHeader);
        messageState.authHeader = authHeader;
        messageState.username = username;
        writeSession(MESSAGE_AUTH_KEY, authHeader);
        writeSession(MESSAGE_USER_KEY, username);
        showMessageApp();
        await refreshMessageExplorer();
    } catch (error) {
        messageElements.messageLoginError.textContent = error.status === 401 ? "Invalid credentials." : error.message;
    } finally {
        messageElements.messageLoginButton.disabled = false;
    }
}

function messageLogout() {
    messageState.authHeader = "";
    messageState.username = "";
    messageState.groups = [];
    messageState.offsets = [];
    messageState.selectedGroupKey = "";
    messageState.selectedShardId = "";
    messageState.cursor = null;
    removeSession(MESSAGE_AUTH_KEY);
    removeSession(MESSAGE_USER_KEY);
    showMessageLogin();
}

async function refreshMessageExplorer() {
    try {
        clearMessageError();
        const response = await messageApiRequest("/groups");
        messageState.groups = response.groups || [];
        if (!messageState.selectedGroupKey && messageState.groups.length > 0) {
            messageState.selectedGroupKey = groupKey(messageState.groups[0]);
        }
        renderMessageGroups();
        await refreshSelectedMessageGroup();
    } catch (error) {
        showMessageError(error.message || "Failed to refresh message explorer.");
    }
}

async function refreshSelectedMessageGroup() {
    const group = selectedMessageGroup();
    if (!group) {
        messageState.offsets = [];
        renderMessageShards();
        renderSelectedOffset();
        renderMessagePage({ records: [], nextCursor: null }, true);
        return;
    }

    try {
        clearMessageError();
        const offsets = await messageApiRequest(`${groupPath(group)}/offsets`);
        messageState.offsets = offsets.shards || [];
        if (!messageState.selectedShardId && messageState.offsets.length > 0) {
            messageState.selectedShardId = shardIdentity(messageState.offsets[0].shard);
        }
        renderMessageShards();
        renderSelectedOffset();
        await loadMessagePage(true);
    } catch (error) {
        showMessageError(error.message || "Failed to load shard offsets.");
    }
}

async function loadMessagePage(reset) {
    const group = selectedMessageGroup();
    const offset = selectedOffset();
    if (!group || !offset) return;
    if (reset) messageState.cursor = null;

    const params = new URLSearchParams({
        direction: messageElements.messageDirection.value,
        limit: messageElements.messageLimit.value,
    });
    if (messageState.cursor) {
        params.set("cursor", messageState.cursor);
    }

    try {
        clearMessageError();
        const shard = offset.shard;
        const page = await messageApiRequest(
            `${groupPath(group)}/shards/${shard.shardIndex}/messages?${params}`,
        );
        messageState.cursor = page.nextCursor;
        renderMessagePage(page, reset);
    } catch (error) {
        showMessageError(error.message || "Failed to load stream records.");
    }
}

function renderMessageGroups() {
    if (messageState.groups.length === 0) {
        messageElements.messageGroupSelect.innerHTML = `<option value="">No groups</option>`;
        return;
    }
    messageElements.messageGroupSelect.innerHTML = messageState.groups.map((group) => {
        const key = groupKey(group);
        const selected = key === messageState.selectedGroupKey ? " selected" : "";
        return `<option value="${escapeAttr(key)}"${selected}>${escapeHtml(group.streamPrefix)} / ${escapeHtml(group.consumerGroup)}</option>`;
    }).join("");
}

function renderMessageShards() {
    if (messageState.offsets.length === 0) {
        messageElements.messageShardPicker.innerHTML = `<option value="">No shards</option>`;
        return;
    }
    const selectedExists = messageState.offsets.some((offset) => shardIdentity(offset.shard) === messageState.selectedShardId);
    if (!selectedExists) {
        messageState.selectedShardId = shardIdentity(messageState.offsets[0].shard);
    }
    messageElements.messageShardPicker.innerHTML = messageState.offsets.map((offset) => {
        const id = shardIdentity(offset.shard);
        const selected = id === messageState.selectedShardId ? " selected" : "";
        return `<option value="${escapeAttr(id)}"${selected}>${escapeHtml(offset.streamKey)} / lag ${escapeHtml(valueOrDash(offset.lag))}</option>`;
    }).join("");
}

function renderSelectedOffset() {
    const group = selectedMessageGroup();
    const offset = selectedOffset();
    messageElements.messageExplorerSubtitle.textContent = group
        ? `${group.streamPrefix} / ${group.consumerGroup}`
        : "Select a stream, consumer group, and shard.";
    messageElements.messageStreamLength.textContent = valueOrDash(offset?.streamLength);
    messageElements.messageLag.textContent = valueOrDash(offset?.lag);
    messageElements.messagePending.textContent = valueOrDash(offset?.pendingCount);
    messageElements.messageEndOffset.textContent = valueOrDash(offset?.lastRecordId || offset?.lastGeneratedId);
    messageElements.messageConsumerOffset.textContent = valueOrDash(
        offset?.consumerLastAckedId || offset?.consumerLastDeliveredId || offset?.groupLastDeliveredId,
    );
    messageElements.messageStreamKey.textContent = offset?.streamKey || "-";
}

function renderMessagePage(page, reset) {
    const rows = (page.records || []).map(messageRow).join("");
    const existingRows = reset ? "" : messageElements.messageRows.innerHTML;
    messageElements.messageRows.innerHTML = existingRows + rows || `<tr><td colspan="4" class="empty-line">No records.</td></tr>`;
    messageElements.messagePageHint.textContent = `${page.direction || messageElements.messageDirection.value}, limit ${page.limit || messageElements.messageLimit.value}`;
    messageElements.messageNextPage.disabled = !page.nextCursor;
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
    if (!value) return "-";
    const date = typeof value === "number" ? new Date(value) : new Date(value);
    if (Number.isNaN(date.getTime())) return escapeHtml(String(value));
    return `<span class="mono">${escapeHtml(date.toLocaleString())}</span>`;
}

async function messageApiRequest(path, authOverride) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), MESSAGE_REQUEST_TIMEOUT_MS);
    let response;
    try {
        response = await fetch(`${MESSAGE_API_BASE}${path}`, {
            headers: {
                "Accept": "application/json",
                "Authorization": authOverride || messageState.authHeader,
            },
            cache: "no-store",
            signal: controller.signal,
        });
    } catch (error) {
        if (error.name === "AbortError") throw new Error("Request timed out.");
        throw error;
    } finally {
        clearTimeout(timeout);
    }
    if (response.status === 401) {
        const error = new Error("Unauthorized");
        error.status = 401;
        throw error;
    }
    if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with HTTP ${response.status}`);
    }
    return response.json();
}

function showMessageLogin() {
    messageElements.messageLoginView.classList.remove("hidden");
    messageElements.messageAppView.classList.add("hidden");
}

function showMessageApp() {
    messageElements.messageLoginView.classList.add("hidden");
    messageElements.messageAppView.classList.remove("hidden");
}

function selectedMessageGroup() {
    return messageState.groups.find((group) => groupKey(group) === messageState.selectedGroupKey);
}

function selectedOffset() {
    return messageState.offsets.find((offset) => shardIdentity(offset.shard) === messageState.selectedShardId);
}

function groupKey(group) {
    return `${group.streamPrefix}::${group.consumerGroup}`;
}

function groupPath(group) {
    return `/streams/${encodeURIComponent(group.streamPrefix)}/groups/${encodeURIComponent(group.consumerGroup)}`;
}

function shardIdentity(shard) {
    return `${shard.shardIndex}`;
}

function createBasicAuth(username, password) {
    const bytes = new TextEncoder().encode(`${username}:${password}`);
    let binary = "";
    bytes.forEach((byte) => {
        binary += String.fromCharCode(byte);
    });
    return `Basic ${btoa(binary)}`;
}

function readSession(key) {
    try {
        return window.localStorage.getItem(key) || "";
    } catch {
        return "";
    }
}

function writeSession(key, value) {
    try {
        window.localStorage.setItem(key, value);
    } catch {
        // Ignore storage failures in embedded dashboards.
    }
}

function removeSession(key) {
    try {
        window.localStorage.removeItem(key);
    } catch {
        // Ignore storage failures in embedded dashboards.
    }
}

function showMessageError(message) {
    messageElements.messageError.textContent = message;
}

function clearMessageError() {
    messageElements.messageError.textContent = "";
}

function valueOrDash(value) {
    return value === null || value === undefined || value === "" ? "-" : String(value);
}

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
    return escapeHtml(value);
}
