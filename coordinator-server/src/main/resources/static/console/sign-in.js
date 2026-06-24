const SIGN_IN_AUTH_KEY = "redisStreamCoordinator.console.auth";
const SIGN_IN_USER_KEY = "redisStreamCoordinator.console.user";
const SIGN_IN_TOKEN_EXPIRES_KEY = "redisStreamCoordinator.console.tokenExpires";
const SIGN_IN_TIMEOUT_MS = 5000;

const signInSections = {
    monitoring: {
        navId: "signInNavMonitoring",
        next: "/console/index.html",
        eyebrow: "Monitoring",
        title: "Redis Stream Coordinator",
        subtitle: "Sign in to open the monitoring console.",
    },
    admin: {
        navId: "signInNavAdmin",
        next: "/console/admin.html",
        eyebrow: "Admin",
        title: "Admin Operations",
        subtitle: "Sign in to manage stream topology and resharding.",
    },
    messages: {
        navId: "signInNavMessages",
        next: "/console/messages.html",
        eyebrow: "Message Explorer",
        title: "Redis Stream Records",
        subtitle: "Sign in to inspect Redis Stream records.",
    },
};

const signInState = {
    next: "/console/index.html",
    section: "monitoring",
};

document.addEventListener("DOMContentLoaded", async () => {
    initializeSignInState();
    renderSignInContext();
    bindSignInForm();
    await redirectIfSessionIsValid();
});

function initializeSignInState() {
    const params = new URLSearchParams(window.location.search);
    const requestedSection = params.get("section") || "monitoring";
    signInState.section = signInSections[requestedSection] ? requestedSection : "monitoring";
    signInState.next = safeNextPath(params.get("next")) || signInSections[signInState.section].next;
}

function renderSignInContext() {
    const context = signInSections[signInState.section];
    document.querySelectorAll(".scalar-nav a").forEach((link) => {
        link.classList.remove("selected");
        link.removeAttribute("aria-current");
    });
    const selected = document.getElementById(context.navId);
    if (selected) {
        selected.classList.add("selected");
        selected.setAttribute("aria-current", "page");
    }
    document.getElementById("signInEyebrow").textContent = context.eyebrow;
    document.getElementById("signInTitle").textContent = context.title;
    document.getElementById("signInSubtitle").textContent = context.subtitle;
}

function bindSignInForm() {
    document.getElementById("signInForm").addEventListener("submit", handleSignIn);
}

async function redirectIfSessionIsValid() {
    const authHeader = readSignInSession(SIGN_IN_AUTH_KEY);
    if (!authHeader) {
        return;
    }
    try {
        await signInRequest("/coord/v1/monitoring/session", {
            authHeader,
            method: "GET",
        });
        window.location.replace(signInState.next);
    } catch (_error) {
        clearSignInSession();
    }
}

async function handleSignIn(event) {
    event.preventDefault();
    const username = document.getElementById("signInUsername").value.trim();
    const password = document.getElementById("signInPassword").value;
    if (!username || !password) {
        showSignInError("Enter username and password.");
        return;
    }

    const button = document.getElementById("signInButton");
    button.disabled = true;
    showSignInError("");
    try {
        const login = await signInRequest("/coord/v1/auth/login", {
            method: "POST",
            body: { username, password },
        });
        writeSignInSession(SIGN_IN_AUTH_KEY, `Bearer ${login.accessToken}`);
        writeSignInSession(SIGN_IN_USER_KEY, username);
        writeSignInSession(SIGN_IN_TOKEN_EXPIRES_KEY, login.expiresAt || "");
        window.location.replace(signInState.next);
    } catch (error) {
        showSignInError(error.status === 401 ? "Invalid credentials." : error.message);
    } finally {
        button.disabled = false;
    }
}

async function signInRequest(path, options) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), SIGN_IN_TIMEOUT_MS);
    let response;
    const headers = {
        Accept: "application/json",
    };
    if (options.authHeader) {
        headers.Authorization = options.authHeader;
    }
    if (options.body) {
        headers["Content-Type"] = "application/json";
    }

    try {
        response = await fetch(path, {
            method: options.method || "GET",
            headers,
            body: options.body ? JSON.stringify(options.body) : undefined,
            cache: "no-store",
            signal: controller.signal,
        });
    } catch (error) {
        if (error.name === "AbortError") {
            throw new Error("Request timed out. Check coordinator connectivity.");
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
        const error = new Error("This account does not have console access.");
        error.status = 403;
        throw error;
    }
    if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed with HTTP ${response.status}`);
    }
    return response.json();
}

function safeNextPath(value) {
    if (!value) {
        return "";
    }
    try {
        const parsed = new URL(value, window.location.origin);
        if (parsed.origin !== window.location.origin || !parsed.pathname.startsWith("/console/")) {
            return "";
        }
        if (parsed.pathname === "/console/sign-in.html") {
            return "";
        }
        return `${parsed.pathname}${parsed.search}${parsed.hash}`;
    } catch (_error) {
        return "";
    }
}

function showSignInError(message) {
    document.getElementById("signInError").textContent = message;
}

function readSignInSession(key) {
    try {
        return window.localStorage.getItem(key) || window.sessionStorage.getItem(key) || "";
    } catch (_error) {
        return "";
    }
}

function writeSignInSession(key, value) {
    try {
        window.localStorage.setItem(key, value);
    } catch (_error) {
        // Browser storage can be disabled. The page still shows the error response.
    }
}

function clearSignInSession() {
    [SIGN_IN_AUTH_KEY, SIGN_IN_USER_KEY, SIGN_IN_TOKEN_EXPIRES_KEY].forEach((key) => {
        try {
            window.localStorage.removeItem(key);
            window.sessionStorage.removeItem(key);
        } catch (_error) {
            // Ignore storage cleanup failures.
        }
    });
}
