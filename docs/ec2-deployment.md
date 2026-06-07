# EC2 Deployment Runbook

This document captures the current manual deployment flow for the shared EC2 environment.

## Scope

The EC2 environment runs:

| Public host | Backend |
| --- | --- |
| `coordinator.ghkdqhrbals.org` | `rsc-coordinator:8080` |
| `monitor.ghkdqhrbals.org` | `rsc-grafana:3000` |
| `api.ghkdqhrbals.org` | BuddyStuddy backend |

Do not repoint `api.ghkdqhrbals.org` to Redis Stream Coordinator.

## TLS and Nginx Routing

The EC2 host must serve three public hostnames with three independent TLS certificates:

| Public host | Certificate name | Backend |
| --- | --- | --- |
| `coordinator.ghkdqhrbals.org` | `coordinator.ghkdqhrbals.org` | `http://127.0.0.1:18080` or `http://rsc-coordinator:8080` |
| `monitor.ghkdqhrbals.org` | `monitor.ghkdqhrbals.org` | `http://127.0.0.1:3001` or `http://rsc-grafana:3000` |
| `api.ghkdqhrbals.org` | `api.ghkdqhrbals.org` | BuddyStuddy backend |

All three DNS records should point to the EC2 Elastic IP. Do not use one certificate that only covers a different hostname; browsers and curl will reject it with a hostname mismatch.

Issue or repair each certificate separately:

```bash
sudo certbot certonly --nginx \
  --cert-name coordinator.ghkdqhrbals.org \
  -d coordinator.ghkdqhrbals.org

sudo certbot certonly --nginx \
  --cert-name monitor.ghkdqhrbals.org \
  -d monitor.ghkdqhrbals.org

sudo certbot certonly --nginx \
  --cert-name api.ghkdqhrbals.org \
  -d api.ghkdqhrbals.org
```

Use one `server` block per hostname so SNI selects the matching certificate:

```nginx
server {
    listen 80;
    server_name coordinator.ghkdqhrbals.org monitor.ghkdqhrbals.org api.ghkdqhrbals.org;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name coordinator.ghkdqhrbals.org;

    ssl_certificate /etc/letsencrypt/live/coordinator.ghkdqhrbals.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/coordinator.ghkdqhrbals.org/privkey.pem;

    location / {
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_pass http://127.0.0.1:18080;
    }
}

server {
    listen 443 ssl http2;
    server_name monitor.ghkdqhrbals.org;

    ssl_certificate /etc/letsencrypt/live/monitor.ghkdqhrbals.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/monitor.ghkdqhrbals.org/privkey.pem;

    location / {
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_pass http://127.0.0.1:3001;
    }
}

server {
    listen 443 ssl http2;
    server_name api.ghkdqhrbals.org;

    ssl_certificate /etc/letsencrypt/live/api.ghkdqhrbals.org/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.ghkdqhrbals.org/privkey.pem;

    location / {
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_pass http://127.0.0.1:<buddystuddy-backend-port>;
    }
}
```

After editing nginx:

```bash
sudo nginx -t
sudo systemctl reload nginx
sudo certbot renew --dry-run
```

Check certificate hostname matching:

```bash
curl -fsS -I https://coordinator.ghkdqhrbals.org/console
curl -fsS -I https://monitor.ghkdqhrbals.org/
curl -fsS -I https://api.ghkdqhrbals.org/
```

## Rules

* Deploy only committed source.
* Do not echo Redis passwords, coordinator passwords, or token secrets.
* Preserve the existing coordinator environment from the running container.
* Keep Grafana data mounted in the existing Docker volume.
* Restart only the service that needs the change.

## Coordinator Image Deploy

From a local checkout:

```bash
git status --short --branch
git archive --format=tar HEAD -o /tmp/rsc-deploy.tar
scp -i "$SSH_KEY" /tmp/rsc-deploy.tar ec2-user@"$EC2_HOST":/tmp/rsc-deploy.tar
```

On EC2:

```bash
rm -rf /tmp/rsc-deploy
mkdir -p /tmp/rsc-deploy
tar -xf /tmp/rsc-deploy.tar -C /tmp/rsc-deploy
cd /tmp/rsc-deploy

DOCKER_BUILDKIT=1 docker build \
  --build-arg APP_TASK=:coordinator-server:bootJar \
  --build-arg APP_LIB_DIR=coordinator-server/build/libs \
  -t redis-stream-coordinator/coordinator:ec2-local \
  .
```

Restart coordinator while preserving its current environment:

```bash
docker inspect rsc-coordinator --format '{{range .Config.Env}}{{println .}}{{end}}' > /tmp/rsc-coordinator.env
docker stop rsc-coordinator
docker rm rsc-coordinator
docker run -d \
  --name rsc-coordinator \
  --network buddystuddy-net \
  --network-alias rsc-coordinator \
  -p 18080:8080 \
  --env-file /tmp/rsc-coordinator.env \
  redis-stream-coordinator/coordinator:ec2-local
docker network connect redis-stream-coordinator-net rsc-coordinator || true
rm -f /tmp/rsc-coordinator.env
```

## Grafana Dashboard Deploy

Copy dashboard JSON files from the same extracted source:

```bash
install -m 0644 /tmp/rsc-deploy/monitoring/grafana/dashboards/*.json \
  /opt/redis-stream-coordinator/grafana/dashboards/
docker restart rsc-grafana
```

## Verification

Use token login instead of embedding `admin:password` in commands:

```bash
TOKEN="$(
  curl -fsS -H 'Content-Type: application/json' \
    -X POST https://coordinator.ghkdqhrbals.org/coord/v1/auth/login \
    -d "{\"username\":\"$COORDINATOR_USERNAME\",\"password\":\"$COORDINATOR_PASSWORD\"}" |
  jq -r '.accessToken'
)"

curl -fsS -H "Authorization: Bearer ${TOKEN}" \
  https://coordinator.ghkdqhrbals.org/coord/v1/monitoring/health
```

Also check:

```bash
curl -fsS -I https://coordinator.ghkdqhrbals.org/console
curl -fsS -I https://monitor.ghkdqhrbals.org/
```

For container-local verification:

```bash
docker ps --format '{{.Names}} {{.Image}} {{.Ports}}'
curl -fsS http://127.0.0.1:18080/actuator/health
```
