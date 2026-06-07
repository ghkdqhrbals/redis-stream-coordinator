# Grafana Dashboard Import

This directory contains Grafana dashboards that can be imported into an existing Grafana instance.

## Requirements

Install these Grafana plugins before importing the full operator dashboards:

* `yesoreyeram-infinity-datasource`
* `gapit-htmlgraphics-panel`

The public overview dashboard only requires:

* `yesoreyeram-infinity-datasource`

It intentionally avoids custom HTML/JavaScript panels so Grafana external/public dashboards can extract and run every panel query on the backend.

## Datasources

Create or provision two datasources:

* Prometheus datasource scraping the coordinator `/actuator/prometheus` endpoint.
* Infinity datasource for the coordinator monitoring API.

For the Infinity datasource, configure:

* URL: coordinator base URL reachable from Grafana, for example `http://coordinator:8080` or `https://api.example.com`.
* Auth: Basic Auth.
* User: coordinator monitoring user, usually a user with `READ`.
* Password: coordinator monitoring password.
* Allowed hosts: the same coordinator base URL.

The password belongs in the Grafana datasource secure settings, not in the dashboard JSON.

For file provisioning, copy `datasources.template.yml` and provide these environment variables:

```bash
export PROMETHEUS_URL=http://prometheus:9090
export COORDINATOR_API_URL=http://coordinator:8080
export COORDINATOR_API_USERNAME=grafana
export COORDINATOR_API_PASSWORD='your-password'
```

## Import Order

Import these dashboard JSON files:

1. `redis-stream-coordinator.json`
2. `redis-stream-coordinator-stream-detail.json`
3. `redis-stream-coordinator-api.json`
4. `redis-stream-coordinator-public.json`

During import, select:

* the Prometheus datasource,
* the Coordinator API Infinity datasource,
* the Coordinator API URL.

The imported dashboards do not pin the repository-local datasource UIDs.

## Public Dashboard

Use `redis-stream-coordinator-public.json` when sharing a dashboard externally.
It contains only Prometheus and Infinity backend-parser panels. It does not include the custom Stream Sharding Overview or Stream Messages panels because those panels fetch data from browser-side JavaScript and Grafana external/public dashboards cannot extract backend queries from them.

For a MacBook Air deployment where Grafana runs in Docker and the coordinator is exposed on the host at port `8080`, configure the Coordinator API datasource as:

```yaml
url: http://host.docker.internal:8080
jsonData:
  auth_method: basicAuth
  allowedHosts:
    - http://host.docker.internal:8080
```

For a public URL deployment, use the public coordinator origin instead:

```yaml
url: https://api.lowfidev.cloud
jsonData:
  auth_method: basicAuth
  allowedHosts:
    - https://api.lowfidev.cloud
```
