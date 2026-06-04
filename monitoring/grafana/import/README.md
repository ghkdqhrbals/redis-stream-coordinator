# Grafana Dashboard Import

This directory contains Grafana dashboards that can be imported into an existing Grafana instance.

## Requirements

Install these Grafana plugins before importing the dashboards:

* `yesoreyeram-infinity-datasource`
* `gapit-htmlgraphics-panel`

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

During import, select:

* the Prometheus datasource,
* the Coordinator API Infinity datasource,
* the Coordinator API URL.

The imported dashboards do not pin the repository-local datasource UIDs.
