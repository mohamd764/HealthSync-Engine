"""
Trainerize client list + weekly compliance extractor.

Writes two tabs:
  * "Trainerize Clients"    - one row per active client (incl. assigned coach)
  * "Trainerize Compliance" - one row per client per week for the last N weeks

API endpoints used:
  POST /v03/user/getClientList
  POST /v03/compliance/getUserCompliance
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from src.extractors.trainerize_client import TrainerizeClient, client_coach_name, client_full_name
from src.utils.sheets_connector import SheetStore

log = logging.getLogger(__name__)

CLIENTS_TAB = "Trainerize Clients"
COMPLIANCE_TAB = "Trainerize Compliance"
COMPLIANCE_WEEKS = 4

CLIENT_HEADERS = [
    "Trainerize ID", "First Name", "Last Name", "Email",
    "Profile Name", "Status", "Role", "Last Signed In",
    "Trial Status", "Assigned Coach",
]

COMPLIANCE_HEADERS = [
    "Client ID", "Client Name", "Profile Name", "Status",
    "Week Start", "Week End",
    "Workouts Scheduled", "Workouts Completed", "Workout Compliance %",
    "Habits Scheduled", "Habits Completed", "Habits Compliance %",
    "Nutrition Completed", "Nutrition Compliance %",
    "Notes",
]


def build_client_rows(clients: list[dict]) -> list[list]:
    return [
        [
            c.get("id", ""),
            c.get("firstName", ""),
            c.get("lastName", ""),
            c.get("email", ""),
            c.get("profileName", ""),
            c.get("status", ""),
            c.get("role", ""),
            c.get("latestSignedIn", ""),
            c.get("trialStatus", ""),
            client_coach_name(c),
        ]
        for c in clients
    ]


def build_compliance_rows(
    clients: list[dict],
    tz: TrainerizeClient,
    weeks: int = COMPLIANCE_WEEKS,
    today: datetime | None = None,
) -> list[list]:
    """Fetch the last ``weeks`` weeks of compliance for each client and flatten to rows."""
    end_date = today or datetime.now()
    start_str = (end_date - timedelta(weeks=weeks)).strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    rows: list[list] = []
    for i, client in enumerate(clients, start=1):
        client_id = client.get("id")
        name = client_full_name(client)
        status = client.get("status", "unknown")
        profile = client.get("profileName", "")
        log.info("  [%d/%d] compliance for client %s", i, len(clients), client_id)

        compliances = tz.get_user_compliance(client_id, start_str, end_str)
        if not compliances:
            rows.append([
                client_id, name, profile, status, start_str, end_str,
                0, 0, 0, 0, 0, 0, 0, None, "No data",
            ])
            continue

        for comp in compliances:
            rows.append([
                client_id, name, profile, status,
                comp.get("startDate", ""), comp.get("endDate", ""),
                comp.get("workoutScheduled", 0) or 0,
                comp.get("workoutCompleted", 0) or 0,
                comp.get("workoutCompliance", 0) or 0,
                comp.get("habitsScheduled", 0) or 0,
                comp.get("habitsCompleted", 0) or 0,
                comp.get("habitsCompliance") or 0,
                comp.get("nutritionCompleted", 0) or 0,
                comp.get("nutritionCompliance") or 0,
                "OK",
            ])
        time.sleep(getattr(tz, "delay", 0))

    return rows


def run(tz: TrainerizeClient, store: SheetStore, weeks: int = COMPLIANCE_WEEKS) -> int:
    log.info("Fetching active Trainerize clients...")
    clients = tz.get_all_clients("activeClient")
    if not clients:
        raise RuntimeError("Trainerize returned no active clients; check TZ_GROUP_ID / TZ_API_TOKEN")
    log.info("Found %d active clients", len(clients))

    store.write(CLIENTS_TAB, [CLIENT_HEADERS] + build_client_rows(clients))

    log.info("Fetching %d-week compliance...", weeks)
    rows = build_compliance_rows(clients, tz, weeks)
    store.write(COMPLIANCE_TAB, [COMPLIANCE_HEADERS] + rows)
    log.info("Wrote %d clients to '%s' and %d rows to '%s'", len(clients), CLIENTS_TAB, len(rows), COMPLIANCE_TAB)
    return len(rows)
