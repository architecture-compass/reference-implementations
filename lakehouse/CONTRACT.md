# Lakehouse contract

Every language implementation runs as one container image with the same commands, writes the same tables, and follows the same rules, so the [checker](check/check.py) and [demo](demo.sh) work unchanged against any of them.

## Commands

The image's entry point is `spark-submit` on the pipeline; the first argument picks the step. Every command first creates the namespaces and tables if they are missing.

| Command | Does |
|---|---|
| `run` (default) | `ingest`, then `merge`, then `aggregate` |
| `ingest` | Kafka topic `shop.public.orders` → `bronze.orders_cdc` |
| `merge` | New rows in `bronze.orders_cdc` → `silver.orders` |
| `aggregate` | `silver.orders` → `gold.daily_revenue` for recent days |
| `maintain` | Compaction, snapshot expiry and orphan-file cleanup on all three tables |

After any command, print the row count of each table.

## Input

Debezium JSON envelopes without schemas, one Kafka message per change:

```json
{"before": null,
 "after": {"order_id": "o-1001", "customer_id": "c-1", "customer_email": "ana@example.com",
           "status": "DELIVERED", "amount": "120.00", "currency": "USD",
           "created_at": "2026-11-10T09:00:00.000000Z"},
 "source": {"ts_ms": 1794460234709, "lsn": 26378376, "...": "..."},
 "op": "r", "ts_ms": 1794460235074}
```

`op` is `r` (initial snapshot), `c` (insert), `u` (update) or `d` (delete). A delete has `after: null` and the key in `before.order_id`. Amounts arrive as strings so no precision is lost.

## Tables (Iceberg catalog `lake`)

**`bronze.orders_cdc`**: one row per change event, append-only, partitioned by `days(ingested_at)`. Columns: `op`, `order_id`, `customer_id`, `status`, `amount DECIMAL(12,2)`, `currency`, `created_at`, `source_ts` (from `source.ts_ms`), `kafka_offset`, `raw` (the full event), `ingested_at`.

**`silver.orders`**: one current row per order, partitioned by `days(created_at)`, merge-on-read. Columns: `order_id`, `customer_id`, `status`, `amount`, `currency`, `created_at`, `updated_at`. **No `customer_email`**: personal data stays in bronze's `raw` column, which only the platform team may read.

**`gold.daily_revenue`**: partitioned by `revenue_date`. Columns: `revenue_date DATE`, `currency`, `orders BIGINT`, `revenue DECIMAL(14,2)`. Counts orders whose status is `PAID`, `SHIPPED` or `DELIVERED`, grouped by UTC date of `created_at` and currency.

## Rules

1. **Incremental and exactly-once.** `ingest` and `merge` are Structured Streaming queries with an *available now* trigger and a checkpoint under `CHECKPOINT_DIR`. Running them again with no new data writes nothing.
2. **Latest change wins.** Within a merge batch, keep one change per order: the highest `source_ts`, then the highest `kafka_offset`.
3. **No going backwards.** Update a silver row only if the change is newer than its `updated_at`; delete only if the delete is at least as new. Replayed or late events never overwrite newer data.
4. **Gold recomputes a window.** Recompute the last 3 days (UTC) and replace exactly that date range, so a day that loses all its revenue is cleared too.
5. **Check before publishing.** Fail `aggregate`, writing nothing, if any gold row has a null date or currency, negative revenue, or a non-positive order count.
6. **Maintenance** runs `rewrite_data_files`, `expire_snapshots` (older than 7 days, keeping at least 20) and `remove_orphan_files` on each table.

## Environment

| Variable | Default |
|---|---|
| `KAFKA_BROKERS` | `kafka:9092` |
| `CDC_TOPIC` | `shop.public.orders` |
| `CHECKPOINT_DIR` | `/checkpoints` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | credentials for the object store |

Catalog and storage settings live in [`infra/spark/spark-defaults.conf`](infra/spark/spark-defaults.conf), shared by every language.
