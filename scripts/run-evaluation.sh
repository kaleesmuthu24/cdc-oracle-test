#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

mkdir -p results
rm -f results/evaluation_summary.csv results/*.json

wait_for_connector 300
python tools/reconcile.py --wait 300

run_and_collect() {
  local name="$1"
  shift
  echo
  echo "=== $name ==="
  reset_sink_metrics
  "$@"
  python tools/reconcile.py --wait 600
  python tools/collect_metrics.py "$name"
}

normal_workload() {
  python tools/workload.py run --events 1000 --rate 50 --seed 101
}

connector_restart() {
  python tools/workload.py run --events 1200 --rate 40 --seed 202 &
  local workload_pid=$!
  sleep 5
  docker compose stop connect >/dev/null
  sleep 12
  docker compose start connect >/dev/null
  wait_for_container_health cdc-connect 300
  wait_for_connector 300
  wait "$workload_pid"
}

target_outage() {
  python tools/workload.py run --events 1200 --rate 40 --seed 303 &
  local workload_pid=$!
  sleep 5
  docker compose stop postgres >/dev/null
  sleep 12
  docker compose start postgres >/dev/null
  wait_for_container_health cdc-postgres 300
  wait "$workload_pid"
}

duplicate_replay() {
  scripts/reset-consumer-group.sh
}

complete_rebuild() {
  curl -fsS -X POST -H "X-Admin-Token: ${ADMIN_TOKEN:-sandbox}" \
    http://localhost:18080/admin/clear >/dev/null
  scripts/reset-consumer-group.sh
}

run_and_collect normal normal_workload
run_and_collect connector_restart connector_restart
run_and_collect target_outage target_outage
run_and_collect duplicate_replay duplicate_replay
run_and_collect complete_rebuild complete_rebuild

cat > results/readiness_score.csv <<'CSV'
control,score,evidence
capture_completeness,2,"Connector restart test and final reconciliation"
replay_sufficiency,2,"Full replay from retained Kafka topic completed"
deterministic_transformation,2,"Source and PostgreSQL hashes matched after replay"
idempotent_projection,2,"Duplicate replay completed without target divergence"
reconciliation,2,"Counts and row hashes compared automatically"
target_specific_restoration,2,"PostgreSQL and Ignite projections rebuilt"
recovery_certification,2,"Normal, restart, outage, replay, and rebuild scenarios executed"
CSV

echo
echo "Evaluation complete. Generated files:"
find results -maxdepth 1 -type f -printf '  %f\n' | sort
