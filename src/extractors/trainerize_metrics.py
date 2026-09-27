"""
Trainerize daily metrics extractor.

Expands each active client's weekly compliance (cardio / workouts / habits) over the
last ``COMPLIANCE_DAYS`` days into one row per client per day and writes the
"Trainerize Daily Logs" tab. Target weight and coach are enriched from the
"New Client Table" and "Weight Analytics" tabs when available.

Body-stat columns (weight, waist, body fat, BMI, resting HR) are kept for schema
compatibility but left empty: weight is sourced from Renpho instead, because
fetching Trainerize body stats requires one API call per client per day and hits
rate limits.
"""
from __future__ import annotations

import csv
import logging
import os
import time
from datetime import datetime, timedelta

from src.extractors.trainerize_client import TrainerizeClient, client_coach_name, client_full_name
from src.utils.client_mapping import CLIENT_TABLE_TAB
from src.utils.sheets_connector import SheetStore

log = logging.getLogger(__name__)

DAILY_LOGS_TAB = "Trainerize Daily Logs"
WEIGHT_ANALYTICS_TAB = "Weight Analytics"
COMPLIANCE_DAYS = 730
CLIENT_TABLE_COACH_COL = 13

HEADERS = [
    "Trainerize ID", "Client Name", "Coach", "Date",
    "Weight (kg)", "Target Weight (kg)", "Waist (cm)", "Body Fat %", "BMI", "Resting HR", "Data Source",
    "Cardio Done", "Cardio Scheduled", "Cardio %",
    "Workouts Done", "Workouts Scheduled", "Workouts %",
    "Habits Done", "Habits Scheduled", "Habits %",
    "Overall Compliance %",
]


def _pct(done: int, scheduled: int):
    return round(done / scheduled * 100) if scheduled else ""


def process_client(
    client: dict,
    tz: TrainerizeClient,
    today: datetime,
    client_metadata: dict[str, dict],
    days: int = COMPLIANCE_DAYS,
) -> list[list]:
    uid = client.get("id")
    name = client_full_name(client)
    meta = client_metadata.get(name.lower(), {})
    target_weight = meta.get("target_weight", "")
    coach = client_coach_name(client) or meta.get("coach", "")

    start_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    compliance_by_week = {c.get("startDate", ""): c for c in tz.get_user_compliance(uid, start_date, end_date)}
    time.sleep(getattr(tz, "delay", 0))

    rows = []
    for d in range(days):
        day = today - timedelta(days=d)
        week_start = (day - timedelta(days=day.weekday())).strftime("%Y-%m-%d")
        comp = compliance_by_week.get(week_start, {})

        cardio_done = comp.get("cardioCompleted", 0) or 0
        cardio_sched = comp.get("cardioScheduled", 0) or 0
        workout_done = comp.get("workoutCompleted", 0) or 0
        workout_sched = comp.get("workoutScheduled", 0) or 0
        habits_done = comp.get("habitsCompleted", 0) or 0
        habits_sched = comp.get("habitsScheduled", 0) or 0

        total_sched = cardio_sched + workout_sched + habits_sched
        if not total_sched:
            continue
        total_done = cardio_done + workout_done + habits_done

        rows.append([
            uid, name, coach, day.strftime("%Y-%m-%d"),
            "", target_weight, "", "", "", "", "",
            cardio_done, cardio_sched, _pct(cardio_done, cardio_sched),
            workout_done, workout_sched, _pct(workout_done, workout_sched),
            habits_done, habits_sched, _pct(habits_done, habits_sched),
            _pct(total_done, total_sched),
        ])

    log.info("    %d days with data (%d compliance weeks)", len(rows), len(compliance_by_week))
    return rows


def load_client_metadata(store: SheetStore) -> dict[str, dict]:
    """Coach names from the New Client Table and target weights from Weight Analytics, keyed by lower-case name."""
    metadata: dict[str, dict] = {}

    for row in store.read(CLIENT_TABLE_TAB)[1:]:
        cname = row[1].strip().lower() if len(row) > 1 else ""
        if not cname:
            continue
        coach = row[CLIENT_TABLE_COACH_COL].strip() if len(row) > CLIENT_TABLE_COACH_COL else ""
        metadata[cname] = {"target_weight": "", "coach": coach}

    # Columns: Client Name | Starting Weight | Latest Weight | Target Weight | ...
    for row in store.read(WEIGHT_ANALYTICS_TAB)[1:]:
        cname = row[0].strip().lower() if row else ""
        if not cname:
            continue
        tw = row[3].strip() if len(row) > 3 and row[3].strip() != "N/A" else ""
        metadata.setdefault(cname, {"target_weight": "", "coach": ""})["target_weight"] = tw

    return metadata


def filter_clients(clients: list[dict], client_filter: str) -> list[dict]:
    needle = client_filter.strip().lower()
    if not needle:
        return clients
    filtered = [c for c in clients if needle in client_full_name(c).lower()]
    if not filtered:
        log.warning("No client matches CLIENT_FILTER; processing all clients")
        return clients
    return filtered


def save_csv(rows: list[list], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(rows)


def run(
    tz: TrainerizeClient,
    store: SheetStore,
    output_dir: str,
    client_filter: str = "",
    today: datetime | None = None,
    days: int = COMPLIANCE_DAYS,
) -> int:
    today = today or datetime.now()

    log.info("Fetching active Trainerize clients...")
    clients = filter_clients(tz.get_all_clients("activeClient"), client_filter)
    if not clients:
        raise RuntimeError("Trainerize returned no active clients; check TZ_GROUP_ID / TZ_API_TOKEN")

    metadata = load_client_metadata(store)
    log.info("Loaded metadata for %d clients; expanding %d days for %d clients", len(metadata), days, len(clients))

    all_rows: list[list] = []
    for i, client in enumerate(clients, start=1):
        log.info("  [%d/%d] client %s", i, len(clients), client.get("id"))
        all_rows.extend(process_client(client, tz, today, metadata, days))

    all_rows.sort(key=lambda r: (r[1], r[3]), reverse=True)
    csv_path = os.path.join(output_dir, "trainerize_daily_logs.csv")
    save_csv(all_rows, csv_path)
    store.write(DAILY_LOGS_TAB, [HEADERS] + all_rows)
    log.info("Wrote %d rows to '%s' (local copy: %s)", len(all_rows), DAILY_LOGS_TAB, csv_path)
    return len(all_rows)
