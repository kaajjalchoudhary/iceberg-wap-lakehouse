"""Command-line entry point: python -m meterlake <command>."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from meterlake.config import Settings
from meterlake.generate import GenerateConfig, generate

log = logging.getLogger("meterlake")


def main(argv: list[str] | None = None) -> int:
    defaults = GenerateConfig()
    parser = argparse.ArgumentParser(prog="meterlake", description="Write-Audit-Publish on Apache Iceberg")
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate", help="write landing files")
    gen.add_argument("--days", type=int, default=defaults.days)
    run = sub.add_parser("run", help="generate landing files, then process every pending batch")
    run.add_argument("--days", type=int, default=defaults.days)
    run.add_argument(
        "--daily-partitions-from", type=date.fromisoformat, default=date(2026, 2, 8),
        help="switch readings from monthly to daily partitions from this batch date on",
    )
    sub.add_parser("process", help="process pending batches already in the landing folder")
    rb = sub.add_parser("rollback", help="move main back to the previous snapshot and rebuild gold")
    rb.add_argument("--to", type=int, default=None, help="snapshot id (default: parent of main)")
    sub.add_parser("maintain", help="compact files, rewrite manifests, expire snapshots")
    sub.add_parser("report", help="print a summary of the lake")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    s = Settings.from_env()

    if args.command in ("generate", "run"):
        log.info("generated %s", generate(s.landing, GenerateConfig(days=args.days)))
        if args.command == "generate":
            return 0

    from meterlake import ingest, maintenance, report, rollback
    from meterlake.spark import get_spark

    spark = get_spark(s.warehouse)
    try:
        if args.command in ("run", "process"):
            evolve_from = getattr(args, "daily_partitions_from", None)
            ingest.run_pending(spark, s, daily_partitions_from=evolve_from)
            print(report.build_report(spark, s))
        elif args.command == "rollback":
            print(rollback.rollback(spark, s, args.to))
        elif args.command == "maintain":
            maintenance.maintain(spark, s)
        elif args.command == "report":
            print(report.build_report(spark, s))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
