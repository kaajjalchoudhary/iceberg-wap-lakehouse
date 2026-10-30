"""Summary of the lake, mostly read from Iceberg metadata tables."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from meterlake.config import Settings
from meterlake.tables import partition_spec


def _show(df: DataFrame, n: int = 40) -> str:
    rows = df.limit(n).collect()
    if not rows:
        return "  (empty)"
    cols = df.columns
    widths = [max(len(c), *(len(str(r[c])) for r in rows)) for c in cols]

    def line(values) -> str:
        return "  " + "  ".join(str(v).ljust(w) for v, w in zip(values, widths, strict=True))

    return "\n".join([line(cols), line("-" * w for w in widths), *(line([r[c] for c in cols]) for r in rows)])


def build_report(spark: SparkSession, s: Settings) -> str:
    readings = s.table("readings")
    out = ["", "Batches (batch_log)"]
    out.append(_show(
        spark.table(s.table("batch_log"))
        .select("batch_id", "status", "rows_received", "rows_added", F.coalesce("failed_audits", F.lit("")).alias("failed"))
        .orderBy("batch_id")
    ))

    out += ["", "Usage by region (gold daily_usage)", _show(
        spark.table(s.table("daily_usage"))
        .groupBy("region")
        .agg(
            F.countDistinct("meter_id").alias("meters"),
            F.round(F.sum("kwh"), 1).alias("kwh"),
            F.round(F.avg("completeness"), 4).alias("avg_completeness"),
        )
        .orderBy("region")
    )]

    out += ["", f"Partition spec now: {partition_spec(spark, readings)}"]
    out += ["Data files by partition spec (readings.files)", _show(
        spark.table(f"{readings}.files")
        .groupBy("spec_id")
        .agg(F.count("*").alias("files"), F.sum("record_count").alias("rows"))
        .orderBy("spec_id")
    )]
    out += ["", f"Columns: {spark.table(readings).columns}"]
    out += ["", "Recent snapshots (readings.snapshots)", _show(
        spark.table(f"{readings}.snapshots")
        .select("committed_at", "snapshot_id", "operation", F.col("summary")["added-records"].alias("added_records"))
        .orderBy(F.col("committed_at").desc()),
        n=6,
    )]
    return "\n".join(out)
