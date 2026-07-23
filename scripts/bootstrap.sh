#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

if ! docker info >/dev/null 2>&1; then
  echo "Docker is not available. In Codespaces, rebuild the dev container so Docker-in-Docker is installed." >&2
  exit 1
fi

mkdir -p results

echo "Starting the sandbox. Oracle initialization can take several minutes on the first run."
docker compose up -d --build

wait_for_container_health cdc-oracle 900
wait_for_container_health cdc-kafka 300
wait_for_container_health cdc-postgres 300
wait_for_container_health cdc-ignite 300
wait_for_container_health cdc-connect 300
wait_for_container_health cdc-sink-app 300

echo "Creating the synthetic baseline before the Debezium initial snapshot..."
python tools/workload.py seed --orders "${SEED_ORDERS:-1000}"

scripts/register-connector.sh

wait_for_url http://localhost:18080/health 300
python tools/reconcile.py --wait 600 --output results/bootstrap-reconciliation.json

echo
echo "Sandbox is ready."
echo "Run: make evaluate"
echo "Metrics: http://localhost:18080/metrics"
echo "Kafka Connect: http://localhost:8083/connectors/oracle-retail-orders/status"
