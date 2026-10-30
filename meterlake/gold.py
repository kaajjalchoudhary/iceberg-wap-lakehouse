"""Gold: daily usage per meter, rebuilt only for the days a published batch touched."""

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from meterlake.config import Settings
from meterlake.generate import INTERVALS_PER_DAY


def refresh_daily_usage(spark: SparkSession, s: Settings, days: list[date]) -> int:
    if not days:
        return 0
    readings = spark.table(s.table("readings")).filter(F.to_date("reading_ts").isin(days))
    meters = spark.table(s.table("meters"))
    usage = (
        readings.join(meters, "meter_id")
        .groupBy(F.to_date("reading_ts").alias("usage_date"), "meter_id", "region", "customer_type")
        .agg(
            F.count("*").alias("readings"),
            F.round(F.sum("kwh"), 3).alias("kwh"),
            F.max("kwh").alias("peak_interval_kwh"),
            F.round(F.count("*") / F.lit(INTERVALS_PER_DAY), 4).alias("completeness"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )
    # Dynamic overwrite: only the usage_date partitions present in `usage` are replaced.
    usage.writeTo(s.table("daily_usage")).overwritePartitions()
    return usage.count()
