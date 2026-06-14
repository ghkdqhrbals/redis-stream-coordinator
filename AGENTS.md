# Redis Stream Coordinator Agent Notes

## Project-Local Skills

Repo-specific agent skills live under [`docs/agent-skills`](docs/agent-skills). Use them before doing the matching work:

* `rsc-docs-refresh`: documentation and release-note updates.
* `rsc-release-maven-publish`: patch/minor version bump and Maven Central publishing.
* `rsc-ship-ec2`: EC2 deployment handoff and verification.

These are project-local skills, not global Codex skills.

## EC2 Deployment

The actual EC2 deployment workflow is managed in the separate `personal-deploy` repository, not in this repository. Use that repository's `Deploy Redis Stream Coordinator` workflow for EC2 deployment.

This repository keeps the coordinator source, Grafana assets, docs, release metadata, and the reference procedure in [`docs/ec2-deployment.md`](docs/ec2-deployment.md). Treat that document as deployment input/reference material for the deploy repository.

When deploying:

* deploy from a committed `feature/coordinator` state,
* do not add an EC2 deployment workflow to this repository,
* do not print Redis or coordinator passwords,
* preserve existing container environment by reading it from the running container,
* rebuild the coordinator image on the EC2 host as `redis-stream-coordinator/coordinator:ec2-local`,
* replace Grafana dashboard JSON files under `/opt/redis-stream-coordinator/grafana/dashboards`,
* restart only the services required by the change,
* verify `https://coordinator.ghkdqhrbals.org/coord/v1/monitoring/health` and `https://monitor.ghkdqhrbals.org/`.

The coordinator domain must route to `rsc-coordinator:8080`, the monitor domain to `rsc-grafana:3000`, and `api.ghkdqhrbals.org` must remain reserved for the separate BuddyStuddy backend.
