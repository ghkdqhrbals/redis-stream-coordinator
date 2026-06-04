(function () {
    const authKey = "redisStreamCoordinator.console.auth";
    const userKey = "redisStreamCoordinator.console.user";

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
            // Ignore storage failures.
        }
    }

    function createBasicAuth(username, password) {
        const bytes = new TextEncoder().encode(`${username}:${password}`);
        let binary = "";
        bytes.forEach((byte) => {
            binary += String.fromCharCode(byte);
        });
        return `Basic ${btoa(binary)}`;
    }

    function setText(id, value) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value || "-";
        }
    }

    function setError(message) {
        const errorElement = document.getElementById("accessError");
        if (errorElement) {
            errorElement.textContent = message || "";
        }
    }

    function loadSession(authHeader) {
        return fetch("/coord/v1/monitoring/session", {
            headers: authHeader ? { Authorization: authHeader } : {},
        }).then((response) => {
            if (!response.ok) {
                throw new Error(response.status === 401 ? "Sign in from the monitoring console first." : `HTTP ${response.status}`);
            }
            return response.json();
        }).then((session) => {
            setText("sessionUsername", session.username || "-");
            setText("sessionRoles", Array.isArray(session.roles) && session.roles.length ? session.roles.join(", ") : "-");
            setError("");
            return session;
        }).catch((error) => {
            setText("sessionUsername", "-");
            setText("sessionRoles", "-");
            setError(error.message);
        });
    }

    const loginForm = document.getElementById("accessLoginForm");
    if (loginForm) {
        loginForm.addEventListener("submit", (event) => {
            event.preventDefault();
            const username = document.getElementById("accessUsername").value.trim();
            const password = document.getElementById("accessPassword").value;
            if (!username || !password) {
                setError("Enter username and password.");
                return;
            }
            const authHeader = createBasicAuth(username, password);
            loadSession(authHeader).then((session) => {
                if (session && session.authenticated) {
                    writeSession(authKey, authHeader);
                    writeSession(userKey, username);
                }
            });
        });
    }

    loadSession(readSession(authKey));
}());
