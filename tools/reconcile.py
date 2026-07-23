from __future__ import annotations

import argparse
import hashlib
import json
import time
from dataclasses import dataclass, asdict

from common import oracle_connection, postgres_connection, sink_metrics


@dataclass
class Reconciliation:
    source_count: int
    target_count: int
    ignite_active_count: int
    source_hash: str
    target_hash: str
    counts_match: bool
    hashes_match: bool
    ignite_count_matches: bool

    @property
    def passed(self) -> bool:
        return self.counts_match and self.hashes_match and self.ignite_count_matches


def digest(rows) -> tuple[int, str]:
    checksum = hashlib.sha256()
    count = 0
    for row in rows:
        canonical = "|".join("" if value is None else str(value) for value in row)
        checksum.update(canonical.encode("utf-8"))
        checksum.update(b"\n")
        count += 1
    return count, checksum.hexdigest()


def reconcile() -> Reconciliation:
    with oracle_connection() as source:
        cursor = source.cursor()
        cursor.execute(
            """
            SELECT ORDER_ID, CUSTOMER_ID, STATUS, TOTAL_CENTS, VERSION_NO
              FROM RETAIL.ORDERS
             ORDER BY ORDER_ID
            """
        )
        source_count, source_hash = digest(cursor)

    with postgres_connection() as target:
        with target.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_id, customer_id, status, total_cents, version_no
                  FROM active_orders
                 ORDER BY order_id
                """
            )
            target_count, target_hash = digest(cursor)

    metrics = sink_metrics()
    ignite_active_count = int(metrics.get("cacheActiveCount", -1))
    return Reconciliation(
        source_count=source_count,
        target_count=target_count,
        ignite_active_count=ignite_active_count,
        source_hash=source_hash,
        target_hash=target_hash,
        counts_match=source_count == target_count,
        hashes_match=source_hash == target_hash,
        ignite_count_matches=source_count == ignite_active_count,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait", type=int, default=0, help="Wait up to N seconds for reconciliation to pass")
    parser.add_argument("--output")
    args = parser.parse_args()

    deadline = time.time() + args.wait
    result = reconcile()
    while not result.passed and time.time() < deadline:
        print(
            f"Waiting: source={result.source_count}, postgres={result.target_count}, "
            f"ignite={result.ignite_active_count}, hashes_match={result.hashes_match}"
        )
        time.sleep(3)
        result = reconcile()

    payload = {**asdict(result), "passed": result.passed}
    print(json.dumps(payload, indent=2))
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
    raise SystemExit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
