#!/usr/bin/env bash
set -euo pipefail

secret_id="${AWS_REDIS_SECRET_ID:-personal/beta}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to read AWS Redis secrets" >&2
  exit 1
fi

secret_json="$(aws secretsmanager get-secret-value \
  --secret-id "$secret_id" \
  --query SecretString \
  --output text)"

export AWS_REDIS_CLUSTER_NODES="$(
  jq -er '."redis.cluster-nodes" // .redisClusterNodes' <<<"$secret_json"
)"
export AWS_REDIS_PASSWORD="$(
  jq -er '."redis.password" // .redisPassword' <<<"$secret_json"
)"

if [ "$#" -eq 0 ]; then
  echo "Loaded Redis secret '$secret_id'. Run a command after this script, for example:"
  echo "scripts/with-aws-redis-secret.sh docker compose -f compose.aws-public-redis.yaml -p rsc-aws-public up -d --build"
  exit 0
fi

exec "$@"
