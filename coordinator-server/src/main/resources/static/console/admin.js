const ADMIN_API_BASE = "/coord/v1";
const ADMIN_MONITORING_BASE = "/coord/v1/monitoring";
const ADMIN_AUTH_KEY = "redisStreamCoordinator.console.auth";
const ADMIN_USER_KEY = "redisStreamCoordinator.console.user";
const ADMIN_TOKEN_EXPIRES_KEY = "redisStreamCoordinator.console.tokenExpires";
const ADMIN_REQUEST_TIMEOUT_MS = 10000;

const adminState = {
    authHeader: readSession(ADMIN_AUTH_KEY),
    username: readSession(ADMIN_USER_KEY),
    tokenExpiresAt: readSession(ADMIN_TOKEN_EXPIRES_KEY),
    groups: [],
    selectedKey: "",
};

const adminElements = {};

document.addEventListener("DOMContentLoaded", () => {
    bindAdminElements();
    bindAdminEvents();
    if (adminState.authHeader) {
        loadAdminSession(adminState.authHeader).then(refreshAdminGroups).catch(showAdminError);
    }
});

function bindAdminElements() {
    [
        "adminLoginForm",
        "adminUsername",
        "adminPassword",
        "adminSessionUsername",
        "adminSessionRoles",
        "adminTokenExpires",
        "adminError",
        "adminGroupSelect",
        "adminRefreshGroups",
        "adminCurrentShardCount",
        "adminSelectedConsumerGroup",
        "adminCreateStreamForm",
        "adminCreateStreamPrefix",
        "adminCreateShardCount",
        "adminScaleForm",
        "adminTargetShardCount",
        "adminScaleReason",
        "adminScaleOut",
        "adminScaleIn",
        "adminResult",
    ].forEach((id) => {
        adminElements[id] = document.getElementById(id);
    });
}

function bindAdminEvents() {
    adminElements.adminLoginForm.addEventListener("submit", handleAdminLogin);
    adminElements.adminRefreshGroups.addEventListener("click", refreshAdminGroups);
    adminElements.adminGroupSelect.addEventListener("change", () => {
        adminState.selectedKey = adminElements.adminGroupSelect.value;
        renderAdminSelection();
    });
    adminElements.adminCreateStreamForm.addEventListener("submit", handleCreateStream);
    adminElements.adminScaleForm.addEventListener("submit", (event) => {
        event.preventDefault();
        applyShardScale(Number(adminElements.adminTargetShardCount.value));
    });
    adminElements.adminScaleOut.addEventListener("click", () => applyShardScale(currentShardCount() + 1));
    adminElements.adminScaleIn.addEventListener("click", () => applyShardScale(Math.max(0, currentShardCount() - 1)));
}

async function handleAdminLogin(event) {
    event.preventDefault();
    const username = adminElements.adminUsername.value.trim();
    const password = adminElements.adminPassword.value;
    if (!username || !password) {
        showAdminError("Enter username and password.");
        return;
    }
    try {
        const login = await loginAdmin(username, password);
        const authHeader = `Bearer ${login.accessToken}`;
        adminState.authHeader = authHeader;
        adminState.username = username;
        adminState.tokenExpiresAt = login.expiresAt || "";
        writeSession(ADMIN_AUTH_KEY, authHeader);
        writeSession(ADMIN_USER_KEY, username);
        writeSession(ADMIN_TOKEN_EXPIRES_KEY, adminState.tokenExpiresAt);
        renderTokenExpiry();
        await loadAdminSession(authHeader);
        await refreshAdminGroups();
        adminElements.adminPassword.value = "";
    } catch (error) {
        showAdminError(error.status === 401 ? "Invalid credentials." : error.message);
    }
}

async function loginAdmin(username, password) {
    return adminRequest("/coord/v1/auth/login", {
        method: "POST",
        authHeader: "",
        body: { username, password },
        baseOverride: "",
    });
}

async function loadAdminSession(authHeader) {
    const session = await adminRequest(`${ADMIN_MONITORING_BASE}/session`, {
        authHeader,
        baseOverride: "",
    });
    adminElements.adminSessionUsername.textContent = session.username || "-";
    adminElements.adminSessionRoles.textContent = Array.isArray(session.roles) ? session.roles.join(", ") : "-";
    renderTokenExpiry();
    showAdminError("");
    return session;
}

async function refreshAdminGroups() {
    const response = await adminRequest(`${ADMIN_MONITORING_BASE}/groups`, { baseOverride: "" });
    adminState.groups = response.groups || [];
    if (!adminState.selectedKey && adminState.groups.length > 0) {
        adminState.selectedKey = groupKey(adminState.groups[0]);
    }
    if (adminState.selectedKey && !adminState.groups.some((group) => groupKey(group) === adminState.selectedKey)) {
        adminState.selectedKey = adminState.groups.length > 0 ? groupKey(adminState.groups[0]) : "";
    }
    renderAdminGroups();
    renderAdminSelection();
}

function renderAdminGroups() {
    if (adminState.groups.length === 0) {
        adminElements.adminGroupSelect.innerHTML = `<option value="">No groups</option>`;
        return;
    }
    adminElements.adminGroupSelect.innerHTML = adminState.groups.map((group) => {
        const key = groupKey(group);
        const selected = key === adminState.selectedKey ? " selected" : "";
        return `<option value="${escapeAttr(key)}"${selected}>${escapeHtml(group.streamPrefix)} / ${escapeHtml(group.consumerGroup)}</option>`;
    }).join("");
}

function renderAdminSelection() {
    const group = selectedGroup();
    adminElements.adminCurrentShardCount.textContent = group ? String(group.shardCount || 0) : "-";
    adminElements.adminSelectedConsumerGroup.textContent = group ? group.consumerGroup : "-";
    if (group) {
        adminElements.adminTargetShardCount.value = String(group.shardCount || 1);
    }
}

async function handleCreateStream(event) {
    event.preventDefault();
    const streamPrefix = adminElements.adminCreateStreamPrefix.value.trim();
    const initialShardCount = Number(adminElements.adminCreateShardCount.value);
    if (!streamPrefix || !Number.isFinite(initialShardCount) || initialShardCount < 1) {
        showAdminError("Enter stream prefix and initial shard count.");
        return;
    }
    const response = await adminRequest(`${ADMIN_API_BASE}/streams/${encodeURIComponent(streamPrefix)}`, {
        method: "POST",
        body: {
            initialShardCount,
            requestedBy: adminState.username || "console-admin",
            reason: "console stream create",
        },
        baseOverride: "",
    });
    renderAdminResult(response);
    await refreshAdminGroups();
}

async function applyShardScale(targetShardCount) {
    const group = selectedGroup();
    if (!group) {
        showAdminError("Select a group first.");
        return;
    }
    if (!Number.isFinite(targetShardCount) || targetShardCount < 0) {
        showAdminError("Target shard count must be 0 or greater.");
        return;
    }
    const response = await adminRequest(`${ADMIN_API_BASE}/streams/${encodeURIComponent(group.streamPrefix)}/scale`, {
        method: "POST",
        body: {
            targetShardCount,
            requestedBy: adminState.username || "console-admin",
            reason: adminElements.adminScaleReason.value.trim() || "console shard scale",
        },
        baseOverride: "",
    });
    renderAdminResult(response);
    await refreshAdminGroups();
}

async function adminRequest(path, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), ADMIN_REQUEST_TIMEOUT_MS);
    const requestedAuthHeader = Object.prototype.hasOwnProperty.call(options, "authHeader")
        ? options.authHeader
        : adminState.authHeader;
    const headers = {
        Accept: "application/json",
    };
    if (requestedAuthHeader) {
        headers.Authorization = requestedAuthHeader;
    }
    if (options.body) {
        headers["Content-Type"] = "application/json";
    }
    try {
        const response = await fetch(path, {
            method: options.method || "GET",
            headers,
            body: options.body ? JSON.stringify(options.body) : undefined,
            cache: "no-store",
            signal: controller.signal,
        });
        if (response.status === 401) {
            const error = new Error("Unauthorized");
            error.status = 401;
            throw error;
        }
        if (!response.ok) {
            throw new Error(await response.text() || `HTTP ${response.status}`);
        }
        return response.json();
    } catch (error) {
        if (error.name === "AbortError") {
            throw new Error("Request timed out.");
        }
        throw error;
    } finally {
        clearTimeout(timeout);
    }
}

function renderAdminResult(value) {
    adminElements.adminResult.textContent = JSON.stringify(value, null, 2);
    showAdminError("");
}

function showAdminError(message) {
    adminElements.adminError.textContent = message?.message || message || "";
}

function selectedGroup() {
    return adminState.groups.find((group) => groupKey(group) === adminState.selectedKey);
}

function currentShardCount() {
    return Math.max(0, Number(selectedGroup()?.shardCount ?? adminElements.adminTargetShardCount.value ?? 0));
}

function groupKey(group) {
    return `${group.streamPrefix}::${group.consumerGroup}`;
}

function readSession(key) {
    try {
        return window.localStorage.getItem(key) || window.sessionStorage.getItem(key) || "";
    } catch {
        return "";
    }
}

function writeSession(key, value) {
    try {
        window.localStorage.setItem(key, value);
    } catch {
        // Ignore browser storage failures.
    }
}

function renderTokenExpiry() {
    const value = adminState.tokenExpiresAt;
    if (!value) {
        adminElements.adminTokenExpires.textContent = "-";
        return;
    }
    const date = new Date(value);
    adminElements.adminTokenExpires.textContent = Number.isNaN(date.getTime()) ? value : date.toLocaleString();
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
