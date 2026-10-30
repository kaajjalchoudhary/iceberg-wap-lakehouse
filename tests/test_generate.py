"""Generator tests. Pure Python, no Spark needed."""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from meterlake.generate import GenerateConfig, generate


def _rows(path: Path) -> list[dict]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def test_deterministic_and_append_only(tmp_path: Path) -> None:
    generate(tmp_path / "a", GenerateConfig(days=4, meters=10))
    generate(tmp_path / "b", GenerateConfig(days=7, meters=10))
    for path in (tmp_path / "a").rglob("*.csv"):
        assert path.read_bytes() == (tmp_path / "b" / path.relative_to(tmp_path / "a")).read_bytes()


def test_bad_day_and_its_redelivery(tmp_path: Path) -> None:
    generate(tmp_path, GenerateConfig(days=7, meters=20, bad_day=5))
    bad = _rows(tmp_path / "readings" / "2026-02-06.csv")
    fixed = _rows(tmp_path / "readings" / "2026-02-06.r2.csv")
    assert len(bad) == len(fixed)
    assert any(float(r["kwh"]) > 100 for r in bad), "Wh reported as kWh"
    assert all(float(r["kwh"]) < 25 for r in fixed)


def test_feed_contains_late_rows_corrections_and_duplicates(tmp_path: Path) -> None:
    generate(tmp_path, GenerateConfig(days=4, meters=30, bad_day=None))
    for path in sorted((tmp_path / "readings").glob("*.csv"))[1:]:
        rows = _rows(path)
        day = path.name[:10]
        assert any(r["reading_ts"][:10] < day for r in rows), "late arrivals"
        assert any(r["revision"] == "2" for r in rows), "corrections"
        keys = Counter((r["meter_id"], r["reading_ts"], r["revision"]) for r in rows)
        assert any(n > 1 for n in keys.values()), "duplicate deliveries"


def test_tariff_column_appears_on_schedule(tmp_path: Path) -> None:
    generate(tmp_path, GenerateConfig(days=5, meters=5, tariff_from_day=3, bad_day=None))
    headers = {p.name: _rows(p)[0].keys() for p in sorted((tmp_path / "readings").glob("*.csv"))}
    assert ["tariff_code" in h for h in headers.values()] == [False, False, False, True, True]


def test_commercial_meters_in_every_region(tmp_path: Path) -> None:
    generate(tmp_path, GenerateConfig(days=1, meters=60))
    meters = _rows(tmp_path / "meters.csv")
    commercial_regions = {m["region"] for m in meters if m["customer_type"] == "commercial"}
    assert commercial_regions == {"North", "South", "East", "West"}
