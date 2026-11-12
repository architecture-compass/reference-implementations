"""Lakehouse pipeline (PySpark).

Kafka change events (Debezium) -> bronze -> silver -> gold, all Apache Iceberg tables.

    spark-submit pipeline.py run        # ingest + merge + aggregate (the default)
    spark-submit pipeline.py ingest     # Kafka -> bronze
    spark-submit pipeline.py merge      # bronze -> silver
    spark-submit pipeline.py aggregate  # silver -> gold
    spark-submit pipeline.py maintain   # compaction, snapshot expiry, orphan-file cleanup

Each streaming step uses an "available now" trigger: it processes everything that arrived
since the last run, records its progress in a checkpoint, and stops. An orchestrator runs
the pipeline every few minutes; nothing is ever processed twice or skipped.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
CDC_TOPIC = os.environ.get("CDC_TOPIC", "shop.public.orders")
CHECKPOINTS = os.environ.get("CHECKPOINT_DIR", "/checkpoints")
GOLD_RECOMPUTE_DAYS = 3
TABLES = ["bronze.orders_cdc", "silver.orders", "gold.daily_revenue"]

spark = SparkSession.builder.appName("lakehouse-python").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def setup() -> None:
    """Create namespaces and tables if they don't exist yet."""
    for ns in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")

    # Every change event, never modified: the system of record for replays.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS bronze.orders_cdc (
            op           STRING,          -- r = snapshot, c = create, u = update, d = delete
            order_id     STRING,
            customer_id  STRING,
            status       STRING,
            amount       DECIMAL(12,2),
            currency     STRING,
            created_at   TIMESTAMP,
            source_ts    TIMESTAMP,       -- when the change was committed in the source
            kafka_offset BIGINT,          -- orders changes to the same order
            raw          STRING,          -- full original event, for replay
            ingested_at  TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(ingested_at))""")

    # One current row per order. No personal data.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.orders (
            order_id     STRING,
            customer_id  STRING,
            status       STRING,
            amount       DECIMAL(12,2),
            currency     STRING,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'merge-on-read',
                       'write.update.mode' = 'merge-on-read', 'write.delete.mode' = 'merge-on-read')""")

    # Business metric for dashboards.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.daily_revenue (
            revenue_date DATE,
            currency     STRING,
            orders       BIGINT,
            revenue      DECIMAL(14,2)
        ) USING iceberg
        PARTITIONED BY (revenue_date)""")


def ingest() -> None:
    """Stream Debezium change events from Kafka into bronze."""
    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BROKERS)
              .option("subscribe", CDC_TOPIC)
              .option("startingOffsets", "earliest")
              .load()
              .select(F.col("value").cast("string").alias("value"), F.col("offset")))

    def field(path: str):
        return F.get_json_object("value", path)

    def after(name: str):
        return field(f"$.after.{name}")

    bronze = events.select(
        field("$.op").alias("op"),
        # Deletes carry the key in "before"; everything else in "after".
        F.coalesce(after("order_id"), field("$.before.order_id")).alias("order_id"),
        after("customer_id").alias("customer_id"),
        after("status").alias("status"),
        after("amount").cast("decimal(12,2)").alias("amount"),
        after("currency").alias("currency"),
        after("created_at").cast("timestamp").alias("created_at"),
        F.timestamp_millis(field("$.source.ts_ms").cast("long")).alias("source_ts"),
        F.col("offset").alias("kafka_offset"),
        F.col("value").alias("raw"),
        F.current_timestamp().alias("ingested_at"),
    )

    (bronze.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_orders")
        .trigger(availableNow=True)
        .toTable("bronze.orders_cdc")
        .awaitTermination())


# Keep only the latest change per order in this batch, then apply it. The updated_at
# guards mean a late or replayed event can never overwrite newer data.
MERGE_SQL = """
    MERGE INTO silver.orders AS t
    USING (
        SELECT * FROM (
            SELECT *, row_number() OVER (
                       PARTITION BY order_id ORDER BY source_ts DESC, kafka_offset DESC) AS rn
            FROM bronze_batch
        ) WHERE rn = 1
    ) AS s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.op = 'd' AND s.source_ts >= t.updated_at THEN DELETE
    WHEN MATCHED AND s.op <> 'd' AND s.source_ts > t.updated_at THEN UPDATE SET
        status = s.status, amount = s.amount, currency = s.currency, updated_at = s.source_ts
    WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT
        (order_id, customer_id, status, amount, currency, created_at, updated_at)
        VALUES (s.order_id, s.customer_id, s.status, s.amount, s.currency, s.created_at, s.source_ts)
"""


def merge() -> None:
    """Read new bronze rows incrementally and merge them into silver."""

    def merge_batch(batch: DataFrame, batch_id: int) -> None:
        batch.createOrReplaceTempView("bronze_batch")
        batch.sparkSession.sql(MERGE_SQL)
        print(f"silver: merged batch {batch_id} ({batch.count()} change events)")

    (spark.readStream.table("bronze.orders_cdc")
        .writeStream
        .foreachBatch(merge_batch)
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_orders")
        .trigger(availableNow=True)
        .start()
        .awaitTermination())


def aggregate() -> None:
    """Recompute recent days of gold.daily_revenue, check them, then publish."""
    start = datetime.now(timezone.utc).date() - timedelta(days=GOLD_RECOMPUTE_DAYS)
    daily = spark.sql(f"""
        SELECT CAST(created_at AS DATE)        AS revenue_date,
               currency,
               count(*)                        AS orders,
               CAST(sum(amount) AS DECIMAL(14,2)) AS revenue
        FROM silver.orders
        WHERE status IN ('PAID', 'SHIPPED', 'DELIVERED')
          AND created_at >= DATE '{start}'
        GROUP BY CAST(created_at AS DATE), currency""").cache()

    # Data-quality gate: nothing is published if a check fails.
    problems = daily.where("revenue_date IS NULL OR currency IS NULL OR revenue < 0 OR orders <= 0").count()
    if problems:
        raise SystemExit(f"gold: {problems} rows failed data-quality checks; nothing published")

    # Replace exactly the recomputed date range, including days that no longer have revenue.
    daily.writeTo("gold.daily_revenue").overwrite(F.col("revenue_date") >= F.lit(start))
    print(f"gold: published {daily.count()} rows for {start} onwards")


def maintain() -> None:
    """Compact small files, expire old snapshots and delete unreferenced files."""
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    for table in TABLES:
        name = f"lake.{table}"
        # Combine small files; streaming writes many. (2 is low, so the demo shows it.)
        rewritten = spark.sql(f"""CALL lake.system.rewrite_data_files(
            table => '{name}', options => map('min-input-files', '2'))""").first()
        # Drop table versions older than a week, but always keep the last 20 for time travel.
        expired = spark.sql(f"""CALL lake.system.expire_snapshots(
            table => '{name}', older_than => TIMESTAMP '{week_ago}', retain_last => 20)""").first()
        # Delete files no snapshot references (left behind by failed writes).
        orphans = spark.sql(f"""CALL lake.system.remove_orphan_files(
            table => '{name}', prefix_listing => true)""").count()
        print(f"maintain {table}: rewrote {rewritten[0]} files into {rewritten[1]}, "
              f"expired {expired[0]} data files, removed {orphans} orphan files")


def counts() -> None:
    for table in TABLES:
        print(f"  {table}: {spark.table(table).count()} rows")


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "run"
    setup()
    steps = {"ingest": [ingest], "merge": [merge], "aggregate": [aggregate],
             "maintain": [maintain], "run": [ingest, merge, aggregate]}
    if command not in steps:
        raise SystemExit(f"unknown command {command!r}; use one of {', '.join(steps)}")
    for step in steps[command]:
        step()
    counts()


if __name__ == "__main__":
    main()
