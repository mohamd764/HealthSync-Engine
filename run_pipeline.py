"""
HealthSync Engine - pipeline entry point.

    python run_pipeline.py                       # full run against real APIs + Google Sheets
    python run_pipeline.py --dry-run             # offline run with fictional data, CSV output
    python run_pipeline.py --steps renpho,merge  # run a subset of steps
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

from src.config import REQUIRED_BY_STEP, ConfigError, Settings
from src.extractors import renpho_api, trainerize_compliance, trainerize_metrics
from src.processors import merge_master
from src.utils.sheets_connector import GoogleSheetStore, LocalCsvStore

STEPS = list(REQUIRED_BY_STEP)
log = logging.getLogger("pipeline")


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Renpho + Trainerize -> Google Sheets -> Looker Studio ETL")
    parser.add_argument("--dry-run", action="store_true",
                        help="use built-in fictional data and write CSVs locally; needs no credentials")
    parser.add_argument("--steps", default=",".join(STEPS),
                        help=f"comma-separated subset of: {', '.join(STEPS)} (default: all, in that order)")
    parser.add_argument("--output-dir", help="directory for local output (default: $OUTPUT_DIR or ./output)")
    parser.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    args = parser.parse_args(argv)

    requested = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = [s for s in requested if s not in STEPS]
    if unknown:
        parser.error(f"unknown step(s): {', '.join(unknown)}")
    args.steps = [s for s in STEPS if s in requested]
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%H:%M:%S")

    settings = Settings.from_env(dotenv=not args.dry_run)
    output_dir = args.output_dir or settings.output_dir
    today = datetime.now()

    if args.dry_run:
        from src import demo

        store = LocalCsvStore(os.path.join(output_dir, "sheets"))
        merger_store = store
        demo.seed_manual_tabs(store, today)
        renpho_source = demo.FakeRenphoFriendsAPI(today)
        tz = demo.FakeTrainerizeClient(today)
        log.info("DRY RUN: fictional data, writing CSVs to %s", store.directory)
    else:
        try:
            settings.validate(args.steps)
        except ConfigError as e:
            log.error("Configuration error: %s. See .env.example.", e)
            return 2
        store = GoogleSheetStore(settings.google_sheet_id, settings.google_credentials_json)
        merger_store = (store if settings.merger_sheet_id == settings.google_sheet_id
                        else GoogleSheetStore(settings.merger_sheet_id, settings.google_credentials_json))
        renpho_source = None
        tz = None
        if "renpho" in args.steps:
            renpho_source = renpho_api.RenphoFriendsAPI(settings.renpho_email, settings.renpho_password)
        if any(s.startswith("trainerize") for s in args.steps):
            from src.extractors.trainerize_client import TrainerizeClient

            tz = TrainerizeClient(settings.tz_group_id, settings.tz_api_token)

    runners = {
        "renpho": lambda: renpho_api.run(renpho_source, store, output_dir),
        "trainerize-compliance": lambda: trainerize_compliance.run(tz, store),
        "trainerize-metrics": lambda: trainerize_metrics.run(
            tz, store, output_dir, client_filter=settings.client_filter, today=today),
        "merge": lambda: merge_master.run(store, merger_store),
    }

    for step in args.steps:
        log.info("=== %s ===", step)
        try:
            rows = runners[step]()
        except Exception:
            log.exception("Step '%s' failed; stopping", step)
            return 1
        log.info("%s: %d rows", step, rows)

    log.info("Pipeline finished (%s)", ", ".join(args.steps))
    return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
