#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

wait_for_url() {
  local url="$1"
  local timeout="${2:-300}"
  local started
  started="$(date +%s)"
  until curl -fsS "$url" >/dev/null 2>&1; do
    if (( $(date +%s) - started > timeout )); then
      echo "Timed out waiting for $url" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_container_health() {
  local container="$1"
  local timeout="${2:-600}"
  local started
  started="$(date +%s)"
  while true; do
    local status
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)"
    if [[ "$status" == "healthy" || "$status" == "running" ]]; then
      return 0
    fi
    if [[ "$status" == "unhealthy" || "$status" == "exited" || "$status" == "dead" ]]; then
      docker logs "$container" --tail 100 || true
      echo "$container entered status $status" >&2
      return 1
    fi
    if (( $(date +%s) - started > timeout )); then
      docker logs "$container" --tail 100 || true
      echo "Timed out waiting for $container health" >&2
      return 1
    fi
    sleep 5
  done
}

connector_state() {
  curl -fsS http://localhost:8083/connectors/oracle-retail-orders/status \
    | python -c 'import json,sys; d=json.load(sys.stdin); print(d.get("connector",{}).get("state","MISSING"))' 2>/dev/null || echo MISSING
}

wait_for_connector() {
  local timeout="${1:-600}"
  local started
  started="$(date +%s)"
  while true; do
    local state
    state="$(connector_state)"
    if [[ "$state" == "RUNNING" ]]; then
      return 0
    fi
    if [[ "$state" == "FAILED" ]]; then
      curl -fsS http://localhost:8083/connectors/oracle-retail-orders/status || true
      return 1
    fi
    if (( $(date +%s) - started > timeout )); then
      curl -fsS http://localhost:8083/connectors/oracle-retail-orders/status || true
      echo "Timed out waiting for connector" >&2
      return 1
    fi
    sleep 3
  done
}

reset_sink_metrics() {
  curl -fsS -X POST -H "X-Admin-Token: ${ADMIN_TOKEN:-sandbox}" \
    http://localhost:18080/admin/reset-metrics >/dev/null
}

wait_for_group_lag_zero() {
  local group_id="${1:-cdc-projection-sink}"
  local topic="${2:-oracle.RETAIL.ORDERS}"
  local timeout="${3:-600}"
  local started observed
  started="$(date +%s)"
  observed=0
  while true; do
    local output lag rows
    output="$(docker exec cdc-kafka /kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server kafka:9092 --group "$group_id" --describe 2>/dev/null || true)"
    rows="$(printf '%s\n' "$output" | awk -v topic="$topic" '$2==topic {count++} END {print count+0}')"
    lag="$(printf '%s\n' "$output" | awk -v topic="$topic" '$2==topic && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')"
    if (( rows > 0 )); then
      observed=1
    fi
    if (( observed == 1 && lag == 0 )); then
      return 0
    fi
    if (( $(date +%s) - started > timeout )); then
      printf '%s\n' "$output" >&2
      echo "Timed out waiting for consumer group $group_id lag to reach zero" >&2
      return 1
    fi
    sleep 3
  done
}
