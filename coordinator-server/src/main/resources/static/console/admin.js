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
    lastCurl: "",
};

const adminElements = {};

document.addEventListener("DOMContentLoaded", () => {
    bindAdminElements();
    bindAdminEvents();
    if (adminState.authHeader) {
        loadAdminSession(adminState.authHeader).then(refreshAdminGroups).catch(() => redirectToSignIn("admin"));
    } else {
        redirectToSignIn("admin");
    }
});

function bindAdminElements() {
    [
        "adminSignOut",
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
        "adminRequestMethod",
        "adminRequestPath",
        "adminCurlPreview",
        "adminCopyCurl",
        "adminResponseStatus",
        "adminResponsePreview",
    ].forEach((id) => {
        adminElements[id] = document.getElementById(id);
    });
}

function bindAdminEvents() {
    adminElements.adminSignOut.addEventListener("click", logoutAdmin);
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
    adminElements.adminCopyCurl.addEventListener("click", copyAdminCurl);
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

function logoutAdmin() {
    adminState.authHeader = "";
    adminState.username = "";
    adminState.tokenExpiresAt = "";
    removeSession(ADMIN_AUTH_KEY);
    removeSession(ADMIN_USER_KEY);
    removeSession(ADMIN_TOKEN_EXPIRES_KEY);
    redirectToSignIn("admin");
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
    renderAdminRequestPreview(path, options, headers);
    try {
        renderAdminResponse("Pending", "Sending request...");
        const response = await fetch(path, {
            method: options.method || "GET",
            headers,
            body: options.body ? JSON.stringify(options.body) : undefined,
            cache: "no-store",
            signal: controller.signal,
        });
        const responseText = await response.text();
        renderAdminResponse(response.status, responseText || response.statusText || "");
        if (response.status === 401) {
            const error = new Error("Unauthorized");
            error.status = 401;
            throw error;
        }
        if (!response.ok) {
            throw new Error(responseText || `HTTP ${response.status}`);
        }
        return responseText ? JSON.parse(responseText) : {};
    } catch (error) {
        if (error.name === "AbortError") {
            renderAdminResponse("Timeout", "Request timed out.");
            throw new Error("Request timed out.");
        }
        if (adminElements.adminResponseStatus?.textContent === "Pending") {
            renderAdminResponse("Error", error.message);
        }
        throw error;
    } finally {
        clearTimeout(timeout);
    }
}

function renderAdminRequestPreview(path, options, headers) {
    const method = options.method || "GET";
    adminElements.adminRequestMethod.textContent = method;
    adminElements.adminRequestMethod.className = `admin-method-pill ${method.toLowerCase()}`;
    adminElements.adminRequestPath.textContent = path;
    adminState.lastCurl = buildAdminCurl(path, method, headers, options.body);
    adminElements.adminCurlPreview.textContent = adminState.lastCurl;
}

function buildAdminCurl(path, method, headers, body) {
    const lines = [`curl ${shellQuote(new URL(path, window.location.origin).toString())}`];
    if (method !== "GET") {
        lines.push(`  --request ${method}`);
    }
    Object.entries(headers).forEach(([name, value]) => {
        const displayValue = name.toLowerCase() === "authorization" ? maskAuthorization(value) : value;
        lines.push(`  --header ${shellQuote(`${name}: ${displayValue}`)}`);
    });
    if (body) {
        lines.push(`  --data ${shellQuote(JSON.stringify(maskSensitivePayload(body), null, 2))}`);
    }
    return lines.join(" \\\n");
}

function maskAuthorization(value) {
    if (!value) {
        return "";
    }
    if (value.startsWith("Bearer ")) {
        const token = value.slice("Bearer ".length);
        return `Bearer ${token.slice(0, 10)}...`;
    }
    return value;
}

function maskSensitivePayload(value) {
    if (Array.isArray(value)) {
        return value.map(maskSensitivePayload);
    }
    if (value && typeof value === "object") {
        return Object.fromEntries(Object.entries(value).map(([key, nested]) => {
            if (isSensitiveField(key)) {
                return [key, "<redacted>"];
            }
            return [key, maskSensitivePayload(nested)];
        }));
    }
    return value;
}

function isSensitiveField(key) {
    return /password|token|secret|credential|authorization/i.test(key);
}

function shellQuote(value) {
    return `'${String(value).replaceAll("'", "'\\''")}'`;
}

function renderAdminResponse(status, body) {
    adminElements.adminResponseStatus.textContent = String(status);
    adminElements.adminResponseStatus.className = `admin-status-pill ${statusClass(status)}`;
    adminElements.adminResponsePreview.textContent = prettyResponseBody(body);
}

function statusClass(status) {
    if (typeof status === "number" && status >= 200 && status < 300) {
        return "ok";
    }
    if (status === "Pending" || status === "Not sent") {
        return "pending";
    }
    return "error";
}

function prettyResponseBody(body) {
    if (!body) {
        return "";
    }
    try {
        return JSON.stringify(JSON.parse(body), null, 2);
    } catch {
        return String(body);
    }
}

async function copyAdminCurl() {
    if (!adminState.lastCurl) {
        return;
    }
    try {
        await navigator.clipboard.writeText(adminState.lastCurl);
        adminElements.adminCopyCurl.textContent = "Copied";
        setTimeout(() => {
            adminElements.adminCopyCurl.textContent = "Copy cURL";
        }, 1200);
    } catch (error) {
        showAdminError("Failed to copy cURL.");
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

function redirectToSignIn(section) {
    const url = new URL("/console/sign-in.html", window.location.origin);
    url.searchParams.set("section", section);
    url.searchParams.set("next", `${window.location.pathname}${window.location.search}${window.location.hash}`);
    window.location.replace(url.toString());
}

function removeSession(key) {
    try {
        window.localStorage.removeItem(key);
        window.sessionStorage.removeItem(key);
    } catch {
        // Ignore browser storage cleanup failures.
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
