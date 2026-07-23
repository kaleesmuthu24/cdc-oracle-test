from __future__ import annotations

import argparse
import random
import time
from datetime import datetime

from common import oracle_connection

STATUSES = ("CREATED", "PAID", "ALLOCATED", "SHIPPED", "COMPLETED", "CANCELLED")


def current_max_id(cursor) -> int:
    cursor.execute("SELECT COALESCE(MAX(ORDER_ID), 0) FROM RETAIL.ORDERS")
    return int(cursor.fetchone()[0])


def seed(order_count: int, batch_size: int) -> None:
    with oracle_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM RETAIL.ORDERS")
        existing = int(cursor.fetchone()[0])
        if existing:
            print(f"Seed skipped: RETAIL.ORDERS already contains {existing} rows.")
            return

        sql = """
            INSERT INTO RETAIL.ORDERS
              (ORDER_ID, CUSTOMER_ID, STATUS, TOTAL_CENTS, VERSION_NO, UPDATED_AT)
            VALUES (:1, :2, :3, :4, :5, :6)
        """
        rows = []
        for order_id in range(1, order_count + 1):
            rows.append((
                order_id,
                ((order_id - 1) % max(100, order_count // 10)) + 1,
                "CREATED",
                500 + ((order_id * 37) % 250_000),
                1,
                datetime.utcnow(),
            ))
            if len(rows) >= batch_size:
                cursor.executemany(sql, rows)
                connection.commit()
                rows.clear()
        if rows:
            cursor.executemany(sql, rows)
            connection.commit()
        print(f"Seeded {order_count} orders.")


def run_workload(events: int, rate: float, seed_value: int, delete_percent: float,
                 insert_percent: float) -> None:
    randomizer = random.Random(seed_value)
    interval = 1.0 / rate if rate > 0 else 0
    started = time.perf_counter()

    with oracle_connection() as connection:
        cursor = connection.cursor()
        max_id = current_max_id(cursor)

        for index in range(events):
            event_started = time.perf_counter()
            choice = randomizer.random() * 100

            if max_id == 0 or choice < insert_percent:
                max_id += 1
                cursor.execute(
                    """
                    INSERT INTO RETAIL.ORDERS
                      (ORDER_ID, CUSTOMER_ID, STATUS, TOTAL_CENTS, VERSION_NO, UPDATED_AT)
                    VALUES (:1, :2, 'CREATED', :3, 1, SYSTIMESTAMP)
                    """,
                    (max_id, randomizer.randint(1, max(100, max_id // 10)), randomizer.randint(500, 250_000)),
                )
            elif choice < insert_percent + delete_percent:
                cursor.execute("SELECT ORDER_ID FROM RETAIL.ORDERS ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY")
                row = cursor.fetchone()
                if row:
                    cursor.execute("DELETE FROM RETAIL.ORDERS WHERE ORDER_ID = :1", (int(row[0]),))
            else:
                cursor.execute("SELECT ORDER_ID FROM RETAIL.ORDERS ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROW ONLY")
                row = cursor.fetchone()
                if row:
                    order_id = int(row[0])
                    cursor.execute(
                        """
                        UPDATE RETAIL.ORDERS
                           SET STATUS = :1,
                               TOTAL_CENTS = TOTAL_CENTS + :2,
                               VERSION_NO = VERSION_NO + 1,
                               UPDATED_AT = SYSTIMESTAMP
                         WHERE ORDER_ID = :3
                        """,
                        (randomizer.choice(STATUSES), randomizer.randint(-100, 500), order_id),
                    )

            connection.commit()
            if interval:
                elapsed = time.perf_counter() - event_started
                if elapsed < interval:
                    time.sleep(interval - elapsed)

            if (index + 1) % max(1, events // 10) == 0:
                print(f"Generated {index + 1}/{events} transactions")

    total = time.perf_counter() - started
    print(f"Completed {events} transactions in {total:.2f}s ({events / total:.2f} tx/s).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic Oracle retail workload")
    subparsers = parser.add_subparsers(dest="command", required=True)

    seed_parser = subparsers.add_parser("seed")
    seed_parser.add_argument("--orders", type=int, default=1000)
    seed_parser.add_argument("--batch-size", type=int, default=250)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--events", type=int, default=1000)
    run_parser.add_argument("--rate", type=float, default=50)
    run_parser.add_argument("--seed", type=int, default=42)
    run_parser.add_argument("--delete-percent", type=float, default=5)
    run_parser.add_argument("--insert-percent", type=float, default=15)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "seed":
        seed(args.orders, args.batch_size)
    else:
        run_workload(args.events, args.rate, args.seed, args.delete_percent, args.insert_percent)


if __name__ == "__main__":
    main()
