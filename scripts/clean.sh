#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down -v --remove-orphans
rm -f results/*.csv results/*.json results/*.log
