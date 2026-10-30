"""Process landed files one batch at a time through Write-Audit-Publish."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from meterlake import audits, gold, tables, wap
from meterlake.config import Settings

log = logging.getLogger(__name__)

BATCH_LOG_SCHEMA = StructType(
    [
        StructField("batch_id", StringType()),
        StructField("status", StringType()),
        StructField("rows_received", LongType()),
        StructField("rows_added", LongType()),
        StructField("affected_days", StringType()),
        StructField("failed_audits", StringType()),
        StructField("main_snapshot_id", LongType()),
        StructField("processed_at", TimestampType()),
    ]
)


@dataclass
class BatchResult:
    batch_id: str
    status: str
    rows_received: int = 0
    rows_added: int = 0
    affected_days: list[date] = field(default_factory=list)
    audits: list[audits.Audit] = field(default_factory=list)
    columns_added: list[str] = field(default_factory=list)
    main_snapshot_id: int | None = None

    @property
    def failed(self) -> list[str]:
        return [a.name for a in self.audits if not a.passed]


def landed_batches(s: Settings) -> list[Path]:
    """Landing files in delivery order (a re-delivery like X.r2.csv sorts right after X.csv)."""
    return sorted((s.landing / "readings").glob("*.csv"))


def batch_date(path: Path) -> date:
    return date.fromisoformat(path.name[:10])


def done_batches(spark: SparkSession, s: Settings) -> set[str]:
    rows = spark.sql(f"SELECT DISTINCT batch_id FROM {s.table('batch_log')}").collect()
    return {r.batch_id for r in rows}


def load_meters(spark: SparkSession, s: Settings) -> None:
    meters = spark.read.csv(str(s.landing / "meters.csv"), header=True)
    meters.select("meter_id", "region", "customer_type").writeTo(s.table("meters")).overwritePartitions()


def process_batch(spark: SparkSession, s: Settings, path: Path) -> BatchResult:
    batch_id = path.name.removesuffix(".csv")
    result = BatchResult(batch_id=batch_id, status="rejected")
    readings = s.table("readings")
    raw = spark.read.csv(str(path), header=True)  # every column as text

    try:
        result.columns_added = tables.evolve_schema(spark, s, raw.columns)
    except tables.ContractViolation as exc:
        result.audits = [audits.Audit("contract", False, str(exc))]
        _log_batch(spark, s, result)
        return result

    target_columns = tables.table_columns(spark, readings)
    typed = raw.select(
        F.trim("meter_id").alias("meter_id"),
        F.to_timestamp("reading_ts").alias("reading_ts"),
        F.col("kwh").cast("double").alias("kwh"),
        F.col("voltage").cast("double").alias("voltage"),
        F.col("revision").cast("int").alias("revision"),
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
        *[F.col(c) if c in raw.columns else F.lit(None).cast("string").alias(c) for c in tables.OPTIONAL_COLUMNS],
    )
    latest = Window.partitionBy("meter_id", "reading_ts").orderBy(F.col("revision").desc())
    batch = (
        typed.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn")
        .select(*target_columns)
        .cache()
    )
    result.rows_received = raw.count()
    result.affected_days = sorted(r.d for r in batch.select(F.to_date("reading_ts").alias("d")).distinct().collect())
    batch.createOrReplaceTempView("incoming_batch")

    # --- write: merge into an isolated branch ------------------------------------------
    branch = wap.branch_name(batch_id)
    main_rows = spark.table(readings).count()
    wap.create_branch(spark, readings, branch)
    lo, hi = batch.agg(F.min("reading_ts"), F.max("reading_ts")).first()
    spark.sql(
        f"""
        MERGE INTO {wap.on_branch(readings, branch)} AS t
        USING incoming_batch AS s
        ON t.meter_id = s.meter_id AND t.reading_ts = s.reading_ts
           AND t.reading_ts BETWEEN TIMESTAMP '{lo}' AND TIMESTAMP '{hi}'
        WHEN MATCHED AND s.revision > t.revision THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    batch.unpersist()

    # --- audit: on the branch -------------------------------------------------------------
    candidate = wap.read_branch(spark, readings, branch)
    result.audits = audits.run_audits(candidate, main_rows, spark.table(s.table("meters")), result.affected_days)
    result.rows_added = candidate.count() - main_rows

    # --- publish or discard ---------------------------------------------------------------
    if result.failed:
        wap.drop_branch(spark, readings, branch)
        result.main_snapshot_id = wap.ref_snapshot(spark, readings, "main")
        log.warning("batch %s REJECTED, main untouched. failed audits: %s", batch_id, result.failed)
    else:
        result.main_snapshot_id = wap.publish(spark, readings, s.ident("readings"), branch)
        result.status = "published"
        gold.refresh_daily_usage(spark, s, result.affected_days)
        log.info(
            "batch %s published: %d rows received, %d new rows, days %s, main snapshot %s",
            batch_id, result.rows_received, result.rows_added,
            [d.isoformat() for d in result.affected_days], result.main_snapshot_id,
        )
    _log_batch(spark, s, result)
    return result


def _log_batch(spark: SparkSession, s: Settings, r: BatchResult) -> None:
    row = (
        r.batch_id,
        r.status,
        r.rows_received,
        r.rows_added,
        ",".join(d.isoformat() for d in r.affected_days),
        json.dumps([a for a in audits.as_dicts(r.audits) if not a["passed"]]) if r.failed else None,
        r.main_snapshot_id,
        datetime.now(timezone.utc).replace(tzinfo=None),
    )
    spark.createDataFrame([row], BATCH_LOG_SCHEMA).writeTo(s.table("batch_log")).append()


def run_pending(
    spark: SparkSession,
    s: Settings,
    daily_partitions_from: date | None = None,
) -> list[BatchResult]:
    """Process every landed batch that has not been processed yet, in delivery order."""
    tables.create_tables(spark, s)
    load_meters(spark, s)
    done = done_batches(spark, s)
    results = []
    for path in landed_batches(s):
        batch_id = path.name.removesuffix(".csv")
        if batch_id in done:
            continue
        if daily_partitions_from and batch_date(path) >= daily_partitions_from:
            tables.evolve_to_daily_partitions(spark, s)
        results.append(process_batch(spark, s, path))
    return results
