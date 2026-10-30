"""Write-Audit-Publish with Iceberg branches.

    write    the batch is merged into a branch created from `main`; readers of `main` see nothing
    audit    checks run against the branch
    publish  `main` is fast-forwarded to the branch: one atomic metadata commit
    discard  or the branch is dropped, and `main` never saw the bad data
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession

from meterlake.config import CATALOG


def branch_name(batch_id: str) -> str:
    return "audit_" + re.sub(r"[^0-9A-Za-z]+", "_", batch_id)


def create_branch(spark: SparkSession, table: str, branch: str) -> None:
    # OR REPLACE: a branch left behind by a crashed run is reset to the current main.
    spark.sql(f"ALTER TABLE {table} CREATE OR REPLACE BRANCH {branch}")


def on_branch(table: str, branch: str) -> str:
    """Identifier that reads from / writes to a branch, e.g. lake.energy.readings.branch_audit_x."""
    return f"{table}.branch_{branch}"


def read_branch(spark: SparkSession, table: str, branch: str) -> DataFrame:
    return spark.table(on_branch(table, branch))


def ref_snapshot(spark: SparkSession, table: str, ref: str) -> int | None:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{ref}'").first()
    return row.snapshot_id if row else None


def publish(spark: SparkSession, table: str, ident: str, branch: str) -> int:
    """Fast-forward main to the audited branch, drop the branch, return main's new snapshot."""
    spark.sql(f"CALL {CATALOG}.system.fast_forward('{ident}', 'main', '{branch}')")
    drop_branch(spark, table, branch)
    return ref_snapshot(spark, table, "main")


def drop_branch(spark: SparkSession, table: str, branch: str) -> None:
    spark.sql(f"ALTER TABLE {table} DROP BRANCH IF EXISTS {branch}")
