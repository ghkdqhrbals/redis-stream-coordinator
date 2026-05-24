# Security Policy

## Supported Versions

The project is currently pre-1.0. Security fixes target the latest development line until the first stable release is published.

After `1.0.0`, the intended support window is:

| Version line | Support |
| --- | --- |
| Current minor `N` | Security fixes |
| Previous minor `N-1` | Best-effort security fixes |
| Older versions | Unsupported unless documented otherwise |

## Reporting A Vulnerability

Do not open a public issue for vulnerabilities.

Report privately through GitHub Security Advisories for this repository. Include:

* affected version or commit
* reproduction steps
* impact
* suggested mitigation, if known

## Security Boundaries

The coordinator is a control-plane service. It provides Basic Auth, role ACL, audit logs, and optional admin mutation rate limiting. Production deployments should still place it behind trusted network boundaries, TLS termination, and secret management.

The RedisStream starter runs inside application processes. Message payload validation, handler authorization, retry, DLQ, and idempotency are application responsibilities.
