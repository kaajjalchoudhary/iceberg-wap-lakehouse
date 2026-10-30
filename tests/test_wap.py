"""Write-Audit-Publish behaviour on small, hand-written batches."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from meterlake import ingest, rollback, tables, wap  # noqa: E402
from meterlake.config import Settings  # noqa: E402

COLUMNS = ["meter_id", "reading_ts", "kwh", "voltage", "revision"]


def land(s: Settings, name: str, rows: list[tuple], columns: list[str] = COLUMNS) -> Path:
    path = s.landing / "readings" / f"{name}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        writer.writerows(rows)
    return path


@pytest.fixture
def lake(spark, settings: Settings) -> Settings:
    settings.landing.mkdir(parents=True)
    (settings.landing / "meters.csv").write_text(
        "meter_id,region,customer_type\nM1,North,residential\nM2,South,commercial\n"
    )
    tables.create_tables(spark, settings)
    ingest.load_meters(spark, settings)
    return settings


def main_rows(spark, s: Settings) -> list:
    return sorted(
        (r.meter_id, str(r.reading_ts), r.kwh, r.revision) for r in spark.table(s.table("readings")).collect()
    )


def test_good_batch_is_published_and_branch_removed(spark, lake: Settings) -> None:
    path = land(lake, "2026-02-01", [
        ("M1", "2026-02-01 00:00:00", "0.4", "230.1", "1"),
        ("M1", "2026-02-01 00:00:00", "0.4", "230.1", "1"),  # duplicate delivery
        ("M2", "2026-02-01 00:30:00", "2.0", "229.0", "1"),
    ])
    result = ingest.process_batch(spark, lake, path)

    assert result.status == "published", result.audits
    assert len(main_rows(spark, lake)) == 2
    refs = {r.name for r in spark.sql(f"SELECT name FROM {lake.table('readings')}.refs").collect()}
    assert refs == {"main"}
    gold = spark.table(lake.table("daily_usage")).collect()
    assert {(g.meter_id, g.readings) for g in gold} == {("M1", 1), ("M2", 1)}


def test_bad_batch_never_reaches_main(spark, lake: Settings) -> None:
    ingest.process_batch(spark, lake, land(lake, "2026-02-01", [("M1", "2026-02-01 00:00:00", "0.4", "230", "1")]))
    before_rows = main_rows(spark, lake)
    before_snapshot = wap.ref_snapshot(spark, lake.table("readings"), "main")

    result = ingest.process_batch(spark, lake, land(lake, "2026-02-02", [
        ("M1", "2026-02-02 00:00:00", "400.0", "230", "1"),  # Wh reported as kWh
        ("M9", "2026-02-02 00:30:00", "0.3", "230", "1"),  # unknown meter
        ("M2", "2026-02-02 00:10:00", "1.0", "230", "1"),  # off the 30-minute grid
    ]))

    assert result.status == "rejected"
    assert set(result.failed) == {"kwh_in_range", "known_meters", "interval_aligned"}
    assert main_rows(spark, lake) == before_rows
    assert wap.ref_snapshot(spark, lake.table("readings"), "main") == before_snapshot
    log = {r.batch_id: r.status for r in spark.table(lake.table("batch_log")).collect()}
    assert log == {"2026-02-01": "published", "2026-02-02": "rejected"}


def test_corrections_win_and_stale_revisions_do_not(spark, lake: Settings) -> None:
    ingest.process_batch(spark, lake, land(lake, "2026-02-01", [("M1", "2026-02-01 00:00:00", "0.4", "230", "1")]))
    ingest.process_batch(spark, lake, land(lake, "2026-02-02", [("M1", "2026-02-01 00:00:00", "0.5", "230", "2")]))
    ingest.process_batch(spark, lake, land(lake, "2026-02-03", [("M1", "2026-02-01 00:00:00", "9.9", "230", "1")]))
    assert main_rows(spark, lake) == [("M1", "2026-02-01 00:00:00", 0.5, 2)]


def test_new_vendor_column_is_added_and_unknown_column_is_rejected(spark, lake: Settings) -> None:
    ok = ingest.process_batch(spark, lake, land(
        lake, "2026-02-01", [("M1", "2026-02-01 00:00:00", "0.4", "230", "1", "TOU")], COLUMNS + ["tariff_code"]
    ))
    assert ok.status == "published" and ok.columns_added == ["tariff_code"]
    assert "tariff_code" in spark.table(lake.table("readings")).columns

    bad = ingest.process_batch(spark, lake, land(
        lake, "2026-02-02", [("M1", "2026-02-02 00:00:00", "0.4", "230", "1", "x")], COLUMNS + ["firmware"]
    ))
    assert bad.status == "rejected" and bad.failed == ["contract"]


def test_partition_evolution_keeps_old_files_readable(spark, lake: Settings) -> None:
    ingest.process_batch(spark, lake, land(lake, "2026-02-01", [("M1", "2026-02-01 00:00:00", "0.4", "230", "1")]))
    assert tables.partition_spec(spark, lake.table("readings")) == ["months(reading_ts)"]
    assert tables.evolve_to_daily_partitions(spark, lake) is True
    assert tables.evolve_to_daily_partitions(spark, lake) is False  # idempotent
    ingest.process_batch(spark, lake, land(lake, "2026-02-02", [("M1", "2026-02-02 00:00:00", "0.6", "230", "1")]))

    specs = {r.spec_id for r in spark.table(f"{lake.table('readings')}.files").select("spec_id").collect()}
    assert specs == {0, 1}
    assert len(main_rows(spark, lake)) == 2


def test_rollback_undoes_the_last_publish_and_its_gold(spark, lake: Settings) -> None:
    ingest.process_batch(spark, lake, land(lake, "2026-02-01", [("M1", "2026-02-01 00:00:00", "0.4", "230", "1")]))
    after_first = main_rows(spark, lake)
    ingest.process_batch(spark, lake, land(lake, "2026-02-02", [
        ("M1", "2026-02-02 00:00:00", "0.6", "230", "1"),
        ("M1", "2026-02-01 00:00:00", "0.5", "230", "2"),  # correction to day 1
    ]))

    summary = rollback.rollback(spark, lake)

    assert summary["batches"] == ["2026-02-02"]
    assert main_rows(spark, lake) == after_first
    gold = {(str(g.usage_date), g.kwh) for g in spark.table(lake.table("daily_usage")).collect()}
    assert gold == {("2026-02-01", 0.4)}  # day 2 removed, day 1 rebuilt without the correction
    statuses = [r.status for r in spark.table(lake.table("batch_log")).filter("batch_id = '2026-02-02'").collect()]
    assert sorted(statuses) == ["published", "rolled_back"]
