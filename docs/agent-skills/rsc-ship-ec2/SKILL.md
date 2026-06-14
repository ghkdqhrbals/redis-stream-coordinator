---
name: rsc-ship-ec2
description: Ship Redis Stream Coordinator to EC2 through the separate personal-deploy repository workflow.
---

# RSC Ship EC2

Use this project-local skill before EC2 deployment or deployment verification.

## Repository Boundary

The EC2 deployment workflow is not in this repository. It lives in the separate `personal-deploy` repository:

- local path: `../personal-deploy`
- workflow: `.github/workflows/deploy-redis-stream-coordinator.yml`
- workflow name: `Deploy Redis Stream Coordinator`

Do not add EC2 deployment automation to `redis-stream-coordinator`.

## Source Repo Preparation

1. Ensure the Redis Stream Coordinator changes are committed and pushed to `feature/coordinator` or the requested release branch/tag.
2. Ensure Grafana dashboard JSON/static console assets are included in that commit if they changed.
3. Record the source ref or commit SHA to pass to the deploy workflow.

## Deploy Repo Execution

In `personal-deploy`, run the manual Redis Stream Coordinator deploy workflow with:

- `deployment_target=ec2`
- `deployment_component=all`, `coordinator`, `pods`, or `grafana` as requested
- `ref=<source branch/tag/SHA>` unless deploying a PR by `pr_number`

Use component-scoped deploys when possible. Do not print secrets.

## Routing Contract

- `coordinator.ghkdqhrbals.org/*` routes to `rsc-coordinator:8080`
- `monitor.ghkdqhrbals.org/*` routes to `rsc-grafana:3000`
- `api.ghkdqhrbals.org/*` remains reserved for BuddyStuddy backend

## Verification

After the deploy workflow succeeds, verify:

```sh
curl -fsS https://coordinator.ghkdqhrbals.org/coord/v1/monitoring/health
curl -fsS https://monitor.ghkdqhrbals.org/
```
