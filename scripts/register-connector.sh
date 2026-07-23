#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

wait_for_url http://localhost:8083/connectors 300

if curl -fsS http://localhost:8083/connectors/oracle-retail-orders >/dev/null 2>&1; then
  echo "Updating existing Oracle connector configuration..."
  python - <<'PY' >/tmp/oracle-connector-config.json
import json
with open('connect/oracle-connector.json', encoding='utf-8') as f:
    print(json.dumps(json.load(f)['config']))
PY
  curl -fsS -X PUT -H 'Content-Type: application/json' \
    --data @/tmp/oracle-connector-config.json \
    http://localhost:8083/connectors/oracle-retail-orders/config
else
  echo "Registering Oracle connector..."
  curl -fsS -X POST -H 'Content-Type: application/json' \
    --data @connect/oracle-connector.json \
    http://localhost:8083/connectors
fi
printf '\n'
wait_for_connector 600
echo "Oracle connector is RUNNING."
