"""Audits that run on the audit branch before anything is published."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MAX_KWH_PER_INTERVAL = 25.0  # far above any 30-minute reading a home or small business can produce


@dataclass
class Audit:
    name: str
    passed: bool
    detail: str


def run_audits(candidate: DataFrame, current_main_rows: int, meters: DataFrame, days: list) -> list[Audit]:
    """`candidate` is the whole table as it would look after publishing (the branch)."""
    scope = candidate.filter(F.to_date("reading_ts").isin(days)).cache()
    results = []

    duplicates = scope.groupBy("meter_id", "reading_ts").count().filter("count > 1").count()
    results.append(Audit("unique_meter_interval", duplicates == 0, f"{duplicates} duplicated (meter, interval) keys"))

    out_of_range = scope.filter(
        F.col("kwh").isNull() | (F.col("kwh") < 0) | (F.col("kwh") > MAX_KWH_PER_INTERVAL)
    ).count()
    results.append(
        Audit("kwh_in_range", out_of_range == 0, f"{out_of_range} readings outside 0..{MAX_KWH_PER_INTERVAL} kWh")
    )

    unknown = scope.join(meters, "meter_id", "left_anti").count()
    results.append(Audit("known_meters", unknown == 0, f"{unknown} readings for meters not in the reference"))

    on_half_hour = scope.filter((F.minute("reading_ts") % 30 != 0) | (F.second("reading_ts") != 0)).count()
    results.append(Audit("interval_aligned", on_half_hour == 0, f"{on_half_hour} readings off the 30-minute grid"))

    total = candidate.count()
    results.append(Audit("no_rows_lost", total >= current_main_rows, f"branch {total} rows vs main {current_main_rows}"))

    scope.unpersist()
    return results


def as_dicts(audits: list[Audit]) -> list[dict]:
    return [asdict(a) for a in audits]
