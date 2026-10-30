"""End to end: generated feed with a bad delivery, partition evolution, maintenance, time travel."""

from __future__ import annotations

from datetime import date

import pytest

pytest.importorskip("pyspark")

from meterlake import ingest, maintenance, tables  # noqa: E402
from meterlake.config import Settings  # noqa: E402
from meterlake.generate import INTERVALS_PER_DAY, GenerateConfig, generate  # noqa: E402
from meterlake.report import build_report  # noqa: E402


@pytest.fixture(scope="module")
def built(spark, tmp_path_factory, warehouse):
    s = Settings(landing=tmp_path_factory.mktemp("e2e") / "landing", warehouse=warehouse, namespace="e2e")
    generate(s.landing, GenerateConfig(days=6, meters=12, tariff_from_day=2, bad_day=3))
    results = ingest.run_pending(spark, s, daily_partitions_from=date(2026, 2, 5))
    return s, results


def test_only_the_bad_delivery_is_rejected(built) -> None:
    _, results = built
    status = {r.batch_id: r.status for r in results}
    assert status.pop("2026-02-04") == "rejected"
    assert status.pop("2026-02-04.r2") == "published"
    assert set(status.values()) == {"published"}


def test_main_never_contains_the_bad_values(spark, built) -> None:
    s, _ = built
    assert spark.table(s.table("readings")).filter("kwh > 25").count() == 0


def test_gold_matches_published_readings(spark, built) -> None:
    s, _ = built
    readings = spark.table(s.table("readings"))
    gold = spark.table(s.table("daily_usage"))
    assert gold.groupBy().sum("readings").first()[0] == readings.count()
    assert gold.filter(f"completeness > 1 OR readings > {INTERVALS_PER_DAY}").count() == 0


def test_schema_and_partition_evolution_happened(spark, built) -> None:
    s, _ = built
    assert "tariff_code" in spark.table(s.table("readings")).columns
    assert tables.partition_spec(spark, s.table("readings")) == ["days(reading_ts)"]


def test_rerun_skips_processed_batches(spark, built) -> None:
    s, _ = built
    assert ingest.run_pending(spark, s) == []


def test_time_travel_to_before_the_last_publish(spark, built) -> None:
    s, _ = built
    t = s.table("readings")
    snapshots = spark.sql(f"SELECT snapshot_id FROM {t}.history ORDER BY made_current_at").collect()
    older = spark.sql(f"SELECT count(*) AS n FROM {t} VERSION AS OF {snapshots[-2].snapshot_id}").first().n
    assert 0 < older < spark.table(t).count()


def test_maintenance_compacts_and_report_renders(spark, built) -> None:
    s, _ = built
    rows_before = spark.table(s.table("readings")).count()
    summary = maintenance.maintain(spark, s)
    assert summary["data_files_rewritten"] > 0
    assert spark.table(s.table("readings")).count() == rows_before
    text = build_report(spark, s)
    assert "2026-02-04.r2" in text and "kwh_in_range" in text
