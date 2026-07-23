from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from common import sink_metrics
from reconcile import reconcile


FIELDS = [
    "timestamp_utc", "experiment", "passed", "source_count", "target_count",
    "ignite_active_count", "consumed", "applied", "duplicate_or_stale", "deletes",
    "failures", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms"
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("experiment")
    parser.add_argument("--results-dir", default="results")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    metrics = sink_metrics()
    reconciliation = reconcile()

    payload = {
        "timestampUtc": datetime.now(timezone.utc).isoformat(),
        "experiment": args.experiment,
        "metrics": metrics,
        "reconciliation": {
            **reconciliation.__dict__,
            "passed": reconciliation.passed,
        },
    }
    json_path = results_dir / f"{args.experiment}.json"
    json_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    csv_path = results_dir / "evaluation_summary.csv"
    row = {
        "timestamp_utc": payload["timestampUtc"],
        "experiment": args.experiment,
        "passed": reconciliation.passed,
        "source_count": reconciliation.source_count,
        "target_count": reconciliation.target_count,
        "ignite_active_count": reconciliation.ignite_active_count,
        "consumed": metrics.get("consumed", -1),
        "applied": metrics.get("applied", -1),
        "duplicate_or_stale": metrics.get("duplicateOrStale", -1),
        "deletes": metrics.get("deletes", -1),
        "failures": metrics.get("failures", -1),
        "latency_p50_ms": metrics.get("latencyP50Ms", -1),
        "latency_p95_ms": metrics.get("latencyP95Ms", -1),
        "latency_p99_ms": metrics.get("latencyP99Ms", -1),
    }
    write_header = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
