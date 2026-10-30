"""Generate a deterministic smart-meter feed, including one bad delivery.

Landing layout:

    landing/meters.csv                 meter reference data
    landing/readings/YYYY-MM-DD.csv    one file per day: 30-minute interval readings
    landing/readings/YYYY-MM-DD.r2.csv a corrected re-delivery of a bad day

What the pipeline has to cope with:
- ~3% of readings arrive a day late, in the next day's file.
- Some readings are corrected the next day (same meter and timestamp, higher `revision`).
- ~0.5% of rows are delivered twice, and ~0.5% of intervals never arrive.
- From `tariff_from_day` the vendor adds a `tariff_code` column (schema evolution).
- On `bad_day` a firmware bug reports Wh instead of kWh for some meters. The pipeline
  must refuse to publish that file; the vendor then re-delivers a fixed `.r2` file.

Output is deterministic, and generating more days reproduces earlier files byte for byte.
"""

from __future__ import annotations

import csv
import math
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

REGIONS = ["North", "South", "East", "West"]
TARIFFS = ["STD", "TOU", "EV"]
BASE_COLUMNS = ["meter_id", "reading_ts", "kwh", "voltage", "revision"]
INTERVALS_PER_DAY = 48


@dataclass(frozen=True)
class GenerateConfig:
    start: date = date(2026, 2, 1)
    days: int = 10
    meters: int = 60
    seed: int = 11
    tariff_from_day: int = 3
    bad_day: int | None = 5


def _write(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _load_profile(slot: int, commercial: bool) -> float:
    """Typical kWh for a 30-minute slot: evening peak for homes, office hours for businesses."""
    hour = slot / 2
    if commercial:
        return 0.6 + (3.5 if 8 <= hour < 19 else 0.0)
    return 0.15 + 0.5 * math.exp(-((hour - 19.5) ** 2) / 6) + 0.25 * math.exp(-((hour - 7.5) ** 2) / 2)


def generate(landing: Path, cfg: GenerateConfig | None = None) -> dict[str, int]:
    cfg = cfg or GenerateConfig()
    rng = random.Random(cfg.seed)

    meters = []
    for i in range(1, cfg.meters + 1):
        meters.append(
            {
                "meter_id": f"M{i:04d}",
                "region": REGIONS[i % len(REGIONS)],
                # Every 5th meter is a business; i % 4 then spreads them across all regions.
                "customer_type": "commercial" if i % 5 == 0 else "residential",
                "tariff_code": rng.choice(TARIFFS),
            }
        )
    _write(landing / "meters.csv", ["meter_id", "region", "customer_type"], meters)

    carry_over: list[dict] = []  # late rows and corrections destined for the next file
    counts = {"meters": len(meters), "readings": 0, "files": 0}

    for offset in range(cfg.days):
        day = cfg.start + timedelta(days=offset)
        rows, next_day = list(carry_over), []
        midnight = datetime.combine(day, time(0, 0))

        for meter in meters:
            commercial = meter["customer_type"] == "commercial"
            for slot in range(INTERVALS_PER_DAY):
                roll = rng.random()
                kwh = round(max(0.0, _load_profile(slot, commercial) * rng.uniform(0.7, 1.3)), 3)
                row = {
                    "meter_id": meter["meter_id"],
                    "reading_ts": (midnight + timedelta(minutes=30 * slot)).strftime("%Y-%m-%d %H:%M:%S"),
                    "kwh": kwh,
                    "voltage": round(rng.gauss(230, 3), 1),
                    "revision": 1,
                    "tariff_code": meter["tariff_code"],
                }
                if roll < 0.005:
                    continue  # never arrives
                if roll < 0.035:
                    next_day.append(row)  # arrives late
                    continue
                rows.append(row)
                if roll > 0.995:
                    rows.append(dict(row))  # delivered twice
                elif roll > 0.99:
                    fixed = dict(row, revision=2, kwh=round(kwh * rng.uniform(0.85, 1.15), 3))
                    next_day.append(fixed)  # corrected tomorrow

        columns = BASE_COLUMNS + (["tariff_code"] if offset >= cfg.tariff_from_day else [])
        name = day.isoformat()
        if offset == cfg.bad_day:
            # Firmware bug: a fifth of the meters report Wh. The vendor re-delivers a fixed file.
            buggy = {m["meter_id"] for m in meters[:: 5]}
            bad = [dict(r, kwh=round(r["kwh"] * 1000, 1)) if r["meter_id"] in buggy else r for r in rows]
            _write(landing / "readings" / f"{name}.csv", columns, bad)
            _write(landing / "readings" / f"{name}.r2.csv", columns, rows)
            counts["files"] += 2
        else:
            _write(landing / "readings" / f"{name}.csv", columns, rows)
            counts["files"] += 1
        counts["readings"] += len(rows)
        carry_over = next_day

    return counts
