#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

docker compose ps
printf '\nConnector status:\n'
curl -fsS http://localhost:8083/connectors/oracle-retail-orders/status | python -m json.tool || true
printf '\nSink metrics:\n'
curl -fsS http://localhost:18080/metrics | python -m json.tool || true
printf '\nReconciliation:\n'
python tools/reconcile.py || true
