"""Undo the latest change to `main` after the fact.

Audits catch what they are written to catch. When a problem is found later, this moves
`main` back to the previous snapshot (a metadata-only operation), rebuilds gold for the
days the undone batch touched and records the rollback in batch_log.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession

from meterlake import gold, wap
from meterlake.config import CATALOG, Settings

log = logging.getLogger(__name__)


def rollback(spark: SparkSession, s: Settings, to_snapshot: int | None = None) -> dict:
    readings = s.table("readings")
    current = wap.ref_snapshot(spark, readings, "main")
    target = to_snapshot
    if target is None:
        row = spark.sql(f"SELECT parent_id FROM {readings}.snapshots WHERE snapshot_id = {current}").first()
        target = row.parent_id if row else None
    if target is None:
        raise ValueError("main has no earlier snapshot to roll back to")

    undone = spark.sql(
        f"SELECT batch_id, affected_days FROM {s.table('batch_log')} "
        f"WHERE status = 'published' AND main_snapshot_id = {current}"
    ).collect()
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{s.ident('readings')}', {target})")

    days = sorted({date.fromisoformat(d) for r in undone for d in (r.affected_days or "").split(",") if d})
    if days:
        in_list = ", ".join(f"DATE '{d.isoformat()}'" for d in days)
        spark.sql(f"DELETE FROM {s.table('daily_usage')} WHERE usage_date IN ({in_list})")
        gold.refresh_daily_usage(spark, s, days)

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for r in undone:
        spark.sql(
            f"INSERT INTO {s.table('batch_log')} VALUES "
            f"('{r.batch_id}', 'rolled_back', 0, 0, '{r.affected_days}', NULL, {target}, TIMESTAMP '{now}')"
        )
    summary = {"from_snapshot": current, "to_snapshot": target, "batches": [r.batch_id for r in undone], "days": days}
    log.warning("rolled back %s from snapshot %s to %s", readings, current, target)
    return summary
