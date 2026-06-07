# Redis Stream Coordinator Agent Notes

## EC2 Deployment

Shared deployment procedure lives in [`docs/ec2-deployment.md`](docs/ec2-deployment.md).

When deploying:

* deploy from a committed `feature/coordinator` state,
* do not print Redis or coordinator passwords,
* preserve existing container environment by reading it from the running container,
* rebuild the coordinator image on the EC2 host as `redis-stream-coordinator/coordinator:ec2-local`,
* replace Grafana dashboard JSON files under `/opt/redis-stream-coordinator/grafana/dashboards`,
* restart only the services required by the change,
* verify `https://coordinator.ghkdqhrbals.org/coord/v1/monitoring/health` and `https://monitor.ghkdqhrbals.org/`.

The coordinator domain must route to `rsc-coordinator:8080`, the monitor domain to `rsc-grafana:3000`, and `api.ghkdqhrbals.org` must remain reserved for the separate BuddyStuddy backend.
