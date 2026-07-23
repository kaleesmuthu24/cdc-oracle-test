#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

GROUP_ID="${1:-cdc-projection-sink}"
TOPIC="${2:-oracle.RETAIL.ORDERS}"

docker compose stop sink-app >/dev/null
sleep 2
docker exec cdc-kafka /kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group "$GROUP_ID" \
  --topic "$TOPIC" \
  --reset-offsets --to-earliest --execute

docker compose start sink-app >/dev/null
wait_for_container_health cdc-sink-app 300
wait_for_group_lag_zero "$GROUP_ID" "$TOPIC" 600
