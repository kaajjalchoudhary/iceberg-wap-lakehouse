"""Table definitions, schema evolution and partition evolution."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from meterlake.config import Settings

log = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["meter_id", "reading_ts", "kwh", "voltage", "revision"]
# Columns the vendor may add later. They are added to the table on first sight;
# anything not listed here is rejected as an unknown change to the contract.
OPTIONAL_COLUMNS = {"tariff_code": "string"}


class ContractViolation(ValueError):
    pass


def create_tables(spark: SparkSession, s: Settings) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{s.namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.table('readings')} (
            meter_id     string,
            reading_ts   timestamp,
            kwh          double,
            voltage      double,
            revision     int,
            _batch_id    string,
            _ingested_at timestamp
        )
        USING iceberg
        PARTITIONED BY (months(reading_ts))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.wap.enabled' = 'true',
            -- Corrections touch a few rows per file. Merge-on-read writes small delete files
            -- instead of rewriting whole data files; compaction folds the deletes back in.
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read'
        )
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.table('meters')} (
            meter_id string, region string, customer_type string
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.table('daily_usage')} (
            usage_date date, meter_id string, region string, customer_type string,
            readings bigint, kwh double, peak_interval_kwh double, completeness double,
            _built_at timestamp
        ) USING iceberg
        PARTITIONED BY (usage_date)
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.table('batch_log')} (
            batch_id string, status string, rows_received bigint, rows_added bigint,
            affected_days string, failed_audits string, main_snapshot_id bigint,
            processed_at timestamp
        ) USING iceberg
        """
    )
    _ensure_main_branch(spark, s)


def _ensure_main_branch(spark: SparkSession, s: Settings) -> None:
    """Give an empty table an (empty) first snapshot so `main` exists to branch from."""
    if spark.sql(f"SELECT count(*) AS n FROM {s.table('readings')}.snapshots").first().n == 0:
        jvm = spark.sparkContext._jvm
        table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, s.table("readings"))
        table.newAppend().commit()
        log.info("created empty first snapshot on %s", s.table("readings"))


def table_columns(spark: SparkSession, table: str) -> list[str]:
    return spark.table(table).columns


def evolve_schema(spark: SparkSession, s: Settings, incoming: list[str]) -> list[str]:
    """Check an incoming header against the contract; add known optional columns. Returns added."""
    missing = [c for c in REQUIRED_COLUMNS if c not in incoming]
    unknown = [c for c in incoming if c not in REQUIRED_COLUMNS and c not in OPTIONAL_COLUMNS]
    if missing or unknown:
        raise ContractViolation(f"missing={missing} unknown={unknown}")
    existing = table_columns(spark, s.table("readings"))
    added = []
    for column in incoming:
        if column not in existing:
            spark.sql(f"ALTER TABLE {s.table('readings')} ADD COLUMN {column} {OPTIONAL_COLUMNS[column]}")
            added.append(column)
            log.info("schema evolution: added column %s to %s", column, s.table("readings"))
    return added


def partition_spec(spark: SparkSession, table: str) -> list[str]:
    """Current partition transforms, e.g. ['months(reading_ts)']."""
    rows = spark.sql(f"DESCRIBE TABLE {table}").collect()
    return [r.data_type for r in rows if (r.col_name or "").startswith("Part ")]


def evolve_to_daily_partitions(spark: SparkSession, s: Settings) -> bool:
    """Switch readings from monthly to daily partitions. Old files keep their old layout."""
    if partition_spec(spark, s.table("readings")) == ["days(reading_ts)"]:
        return False
    spark.sql(f"ALTER TABLE {s.table('readings')} REPLACE PARTITION FIELD reading_ts_month WITH days(reading_ts)")
    log.info("partition evolution: %s is now partitioned by days(reading_ts)", s.table("readings"))
    return True
