"""Iceberg table maintenance: compaction, manifest rewrite and snapshot expiry."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from meterlake.config import CATALOG, Settings

log = logging.getLogger(__name__)


def maintain(spark: SparkSession, s: Settings, retain_last: int = 5) -> dict:
    ident = s.ident("readings")
    # Compaction also rewrites files written under the old monthly spec into the current daily one.
    options = "map('min-input-files', '2', 'delete-file-threshold', '1')"
    compacted = spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files(table => '{ident}', options => {options})"
    ).first()
    manifests = spark.sql(f"CALL {CATALOG}.system.rewrite_manifests('{ident}')").first()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    expired = spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots(table => '{ident}', "
        f"older_than => TIMESTAMP '{now}', retain_last => {retain_last})"
    ).first()
    summary = {
        "data_files_rewritten": compacted.rewritten_data_files_count,
        "data_files_added": compacted.added_data_files_count,
        "manifests_rewritten": manifests.rewritten_manifests_count,
        "snapshots_expired_data_files": expired.deleted_data_files_count,
    }
    log.info("maintenance on %s: %s", s.table("readings"), summary)
    return summary
