"""Independent checks for the lakehouse demo.

Reads the Iceberg tables with PyIceberg (no Spark), which also shows the point of an open
table format: any engine can read what Spark wrote. Compares them with the source database.

    python check.py state                       # JSON summary; exit 1 if the lake disagrees with the source
    python check.py status-at SNAPSHOT ORDER_ID  # an order's status in an older silver version
"""

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import psycopg
from pyiceberg.catalog import load_catalog

GOLD_RECOMPUTE_DAYS = 3
PAID_STATUSES = ("PAID", "SHIPPED", "DELIVERED")

catalog = load_catalog(
    "lake",
    type="rest",
    uri=os.environ.get("CATALOG_URI", "http://iceberg-rest:8181"),
    **{
        "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333"),
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3.region": os.environ.get("AWS_REGION", "us-east-1"),
    },
)


def utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


def source_orders() -> dict[str, tuple]:
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        rows = conn.execute(
            "SELECT order_id, customer_id, status, amount, currency, created_at FROM orders").fetchall()
    return {r[0]: (r[1], r[2], Decimal(r[3]), r[4], utc(r[5])) for r in rows}


def expected_gold(orders: dict[str, tuple]) -> dict[tuple, tuple]:
    start = datetime.now(timezone.utc).date() - timedelta(days=GOLD_RECOMPUTE_DAYS)
    gold: dict[tuple, list] = {}
    for _, status, amount, currency, created in orders.values():
        day = datetime.fromisoformat(created).date()
        if status in PAID_STATUSES and day >= start:
            entry = gold.setdefault((day.isoformat(), currency), [0, Decimal("0")])
            entry[0] += 1
            entry[1] += amount
    return {k: (v[0], v[1]) for k, v in gold.items()}


def rows(table: str, snapshot_id: int | None = None) -> list[dict]:
    t = catalog.load_table(table)
    scan = t.scan(snapshot_id=snapshot_id) if snapshot_id else t.scan()
    return scan.to_arrow().to_pylist()


def state() -> int:
    source = source_orders()
    silver_table = catalog.load_table("silver.orders")
    silver = {r["order_id"]: (r["customer_id"], r["status"], r["amount"], r["currency"], utc(r["created_at"]))
              for r in silver_table.scan().to_arrow().to_pylist()}
    gold = {(r["revenue_date"].isoformat(), r["currency"]): (r["orders"], r["revenue"])
            for r in rows("gold.daily_revenue")}
    want_gold = expected_gold(source)

    mismatches = [f"silver {k}: lake={silver.get(k)} source={source.get(k)}"
                  for k in sorted(set(source) | set(silver)) if silver.get(k) != source.get(k)]
    mismatches += [f"gold {k}: lake={gold.get(k)} expected={want_gold.get(k)}"
                   for k in sorted(set(gold) | set(want_gold)) if gold.get(k) != want_gold.get(k)]

    snapshot = silver_table.current_snapshot()
    result = {
        "bronze_rows": len(rows("bronze.orders_cdc")),
        "silver_rows": len(silver),
        "source_rows": len(source),
        "gold_rows": len(gold),
        "silver_snapshot": snapshot.snapshot_id if snapshot else None,
        "silver_matches_source": not any(m.startswith("silver") for m in mismatches),
        "gold_matches_source": not any(m.startswith("gold") for m in mismatches),
        "gold": {f"{d} {c}": f"{n} orders, {rev}" for (d, c), (n, rev) in sorted(gold.items())},
        "mismatches": mismatches,
    }
    print(json.dumps(result))
    return 1 if mismatches else 0


def status_at(snapshot_id: str, order_id: str) -> int:
    for r in rows("silver.orders", int(snapshot_id)):
        if r["order_id"] == order_id:
            print(r["status"])
            return 0
    print("ABSENT")
    return 0


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "state"
    if command == "state":
        sys.exit(state())
    if command == "status-at" and len(sys.argv) == 4:
        sys.exit(status_at(sys.argv[2], sys.argv[3]))
    sys.exit(__doc__)
