#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "Missing required environment variable: ${name}" >&2
    exit 2
  fi
}

json_escape() {
  local value="${1:-}"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

parse_shard_count() {
  sed -n 's/.*"shardCount"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n 1
}

curl_status() {
  local method="$1"
  local url="$2"
  local payload="${3:-}"
  local output_file="$4"
  local request_id="$5"
  local args=(-sS -o "$output_file" -w "%{http_code}" -X "$method" -H "Content-Type: application/json" -H "X-Request-Id: ${request_id}")

  if [ -n "${COORDINATOR_USERNAME:-}" ] || [ -n "${COORDINATOR_PASSWORD:-}" ]; then
    args+=(-u "${COORDINATOR_USERNAME:-}:${COORDINATOR_PASSWORD:-}")
  fi

  if [ -n "$payload" ]; then
    args+=(-d "$payload")
  fi

  curl "${args[@]}" "$url"
}

require_env COORDINATOR_BASE_URL
require_env STREAM_PREFIX
require_env CONSUMER_GROUP
require_env SHARD_COUNT

if ! [[ "$SHARD_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "SHARD_COUNT must be a positive integer: ${SHARD_COUNT}" >&2
  exit 2
fi

BASE_URL="${COORDINATOR_BASE_URL%/}"
GROUP_URL="${BASE_URL}/coord/v1/streams/${STREAM_PREFIX}/groups/${CONSUMER_GROUP}"
ROUTING_URL="${GROUP_URL}/producer-routing"
SCALE_URL="${BASE_URL}/coord/v1/streams/${STREAM_PREFIX}/scale"
REQUESTED_BY="${REQUESTED_BY:-terraform}"
REASON="${REASON:-terraform managed shard count}"
REQUEST_ID_PREFIX="${REQUEST_ID_PREFIX:-terraform}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-120}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
REQUEST_SUFFIX="$(date +%s)-${STREAM_PREFIX}-${CONSUMER_GROUP}-${SHARD_COUNT}"

create_body="$(printf '{"initialShardCount":%s,"requestedBy":"%s","reason":"%s"}' "$SHARD_COUNT" "$(json_escape "$REQUESTED_BY")" "$(json_escape "$REASON")")"
scale_body="$(printf '{"targetShardCount":%s,"requestedBy":"%s","reason":"%s"}' "$SHARD_COUNT" "$(json_escape "$REQUESTED_BY")" "$(json_escape "$REASON")")"

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

status="$(curl_status POST "$GROUP_URL" "$create_body" "$tmp" "${REQUEST_ID_PREFIX}-create-${REQUEST_SUFFIX}")"
case "$status" in
  200|201)
    echo "Created coordinator group ${STREAM_PREFIX}/${CONSUMER_GROUP} with ${SHARD_COUNT} shard(s)."
    ;;
  409)
    echo "Coordinator group ${STREAM_PREFIX}/${CONSUMER_GROUP} already exists; checking shard count."
    ;;
  *)
    echo "Create group failed with HTTP ${status}:" >&2
    cat "$tmp" >&2
    exit 1
    ;;
esac

status="$(curl_status GET "$ROUTING_URL" "" "$tmp" "${REQUEST_ID_PREFIX}-routing-before-${REQUEST_SUFFIX}")"
if [ "$status" != "200" ]; then
  echo "Read producer routing metadata failed with HTTP ${status}:" >&2
  cat "$tmp" >&2
  exit 1
fi

current="$(cat "$tmp" | parse_shard_count)"
if [ -z "$current" ]; then
  echo "Could not parse shardCount from producer routing metadata:" >&2
  cat "$tmp" >&2
  exit 1
fi

if [ "$current" = "$SHARD_COUNT" ]; then
  echo "Coordinator group ${STREAM_PREFIX}/${CONSUMER_GROUP} already has ${SHARD_COUNT} shard(s)."
  exit 0
fi

status="$(curl_status POST "$SCALE_URL" "$scale_body" "$tmp" "${REQUEST_ID_PREFIX}-scale-${REQUEST_SUFFIX}")"
case "$status" in
  200|202)
    echo "Requested stream shard count change for ${STREAM_PREFIX}: ${current} -> ${SHARD_COUNT}."
    ;;
  *)
    echo "Scale group failed with HTTP ${status}:" >&2
    cat "$tmp" >&2
    exit 1
    ;;
esac

deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
while true; do
  status="$(curl_status GET "$ROUTING_URL" "" "$tmp" "${REQUEST_ID_PREFIX}-routing-after-${REQUEST_SUFFIX}")"
  if [ "$status" = "200" ]; then
    current="$(cat "$tmp" | parse_shard_count)"
    if [ "$current" = "$SHARD_COUNT" ]; then
      echo "Producer routing metadata now exposes ${SHARD_COUNT} shard(s)."
      exit 0
    fi
    echo "Waiting for producer routing metadata: current=${current:-unknown}, target=${SHARD_COUNT}."
  else
    echo "Waiting for producer routing metadata failed with HTTP ${status}."
  fi

  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "Timed out waiting for shard count ${SHARD_COUNT}." >&2
    cat "$tmp" >&2
    exit 1
  fi

  sleep "$POLL_INTERVAL_SECONDS"
done
