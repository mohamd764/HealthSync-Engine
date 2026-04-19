"""
Trainerize → Google Sheets: Workout Compliance Extraction Script
================================================================
Fetches client list and weekly compliance data from the Trainerize API,
then exports it to a "Trainerize Compliance" tab in the Renpho Master Data sheet.

API Endpoints used:
  POST /v03/user/getClientList  → all active clients
  POST /v03/compliance/getUserCompliance → weekly compliance per client

Authentication: Basic Auth (GroupID:APIToken)

Usage:
  python trainerize_to_sheets.py
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

# ─── Configuration ───────────────────────────────────────────────────────────

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# Trainerize API credentials
TZ_GROUP_ID = os.getenv("TZ_GROUP_ID", "559190")
TZ_API_TOKEN = os.getenv("TZ_API_TOKEN", "sBx8XX3BykCy4v8T1c3jQ")

# Google Sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "YOUR_GOOGLE_SHEET_ID_HERE")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS_JSON", "credentials.json")

# API settings
API_BASE = "https://api.trainerize.com/v03"
COMPLIANCE_WEEKS = 4  # How many weeks of compliance data to fetch
REQUEST_TIMEOUT = 30
DELAY_BETWEEN_REQUESTS = 0.5  # Seconds between API calls to avoid rate limiting


# ─── Trainerize API Client ──────────────────────────────────────────────────

class TrainerizeClient:
    """Simple client for the Trainerize REST API (v03)."""

    def __init__(self, group_id: str, api_token: str):
        auth = base64.b64encode(f"{group_id}:{api_token}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {auth}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.base_url = API_BASE

    def _post(self, endpoint: str, payload: dict) -> dict:
        """Make a POST request to the Trainerize API."""
        url = f"{self.base_url}{endpoint}"
        try:
            resp = requests.post(url, headers=self.headers, json=payload,
                                 timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"  ⚠ Timeout on {endpoint}")
            return {}
        except requests.exceptions.HTTPError as e:
            print(f"  ⚠ HTTP {resp.status_code} on {endpoint}: {resp.text[:200]}")
            return {}
        except Exception as e:
            print(f"  ⚠ Error on {endpoint}: {e}")
            return {}

    def get_all_clients(self, view: str = "allActive") -> list[dict]:
        """
        Fetch all clients. Paginates automatically.
        Returns list of {id, firstName, lastName, email, status, profileName, ...}
        """
        all_clients = []
        start = 0
        page_size = 50

        while True:
            payload = {
                "view": view,
                "sort": "name",
                "start": start,
                "count": page_size,
                "verbose": True,
            }
            print(f"  Fetching clients (start={start})...")
            data = self._post("/user/getClientList", payload)
            users = data.get("users", [])

            if not users:
                break

            all_clients.extend(users)
            print(f"    Got {len(users)} clients (total: {len(all_clients)})")

            if len(users) < page_size:
                break  # Last page

            start += page_size
            time.sleep(DELAY_BETWEEN_REQUESTS)

        return all_clients

    def get_user_compliance(self, user_id: int, start_date: str, end_date: str) -> list[dict]:
        """
        Fetch compliance data for a specific user.
        Returns list of weekly compliance records.
        """
        payload = {
            "userID": user_id,
            "startDate": start_date,
            "endDate": end_date,
        }
        data = self._post("/compliance/getUserCompliance", payload)
        return data.get("compliances", [])


# ─── Data Processing ────────────────────────────────────────────────────────

def build_compliance_rows(clients: list[dict], tz_client: TrainerizeClient,
                          weeks: int = COMPLIANCE_WEEKS) -> list[list]:
    """
    For each client, fetch their compliance data for the last N weeks
    and flatten into rows for Google Sheets.
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=weeks)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    rows = []
    total = len(clients)

    for i, client in enumerate(clients):
        client_id = client.get("id")
        name = f"{client.get('firstName', '')} {client.get('lastName', '')}".strip()
        status = client.get("status", "unknown")
        profile = client.get("profileName", "")

        print(f"  [{i+1}/{total}] Fetching compliance for {name} (ID: {client_id})...")

        compliances = tz_client.get_user_compliance(client_id, start_str, end_str)

        if not compliances:
            # Add a single row showing no data
            rows.append([
                client_id, name, profile, status,
                start_str, end_str,
                0, 0, 0,   # workout: scheduled, completed, compliance %
                0, 0, 0,   # habits
                0, None,   # nutrition
                "No data",
            ])
            continue

        for comp in compliances:
            week_start = comp.get("startDate", "")
            week_end = comp.get("endDate", "")
            w_sched = comp.get("workoutScheduled", 0) or 0
            w_done = comp.get("workoutCompleted", 0) or 0
            w_pct = comp.get("workoutCompliance", 0) or 0
            h_sched = comp.get("habitsScheduled", 0) or 0
            h_done = comp.get("habitsCompleted", 0) or 0
            h_pct = comp.get("habitsCompliance") or 0
            n_done = comp.get("nutritionCompleted", 0) or 0
            n_pct = comp.get("nutritionCompliance") or 0

            rows.append([
                client_id, name, profile, status,
                week_start, week_end,
                w_sched, w_done, w_pct,
                h_sched, h_done, h_pct,
                n_done, n_pct,
                "OK",
            ])

        time.sleep(DELAY_BETWEEN_REQUESTS)

    return rows


# ─── Google Sheets Export ────────────────────────────────────────────────────

def export_compliance_to_sheets(rows: list[list]) -> None:
    """Export compliance data to the 'Trainerize Compliance' tab."""
    import gspread
    from google.oauth2.service_account import Credentials

    creds_path = os.path.join(os.path.dirname(__file__), GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_file(creds_path, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(GOOGLE_SHEET_ID)

    tab_name = "Trainerize Compliance"
    try:
        ws = sheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=2000, cols=20)

    headers = [
        "Client ID", "Client Name", "Profile Name", "Status",
        "Week Start", "Week End",
        "Workouts Scheduled", "Workouts Completed", "Workout Compliance %",
        "Habits Scheduled", "Habits Completed", "Habits Compliance %",
        "Nutrition Completed", "Nutrition Compliance %",
        "Notes",
    ]

    ws.clear()
    ws.update([headers] + rows, "A1")
    ws.format("A1:O1", {"textFormat": {"bold": True}})

    print(f"\n✅ Exported {len(rows)} rows to '{tab_name}' tab in Google Sheets!")


def export_clients_to_sheets(clients: list[dict]) -> None:
    """Export client list to the 'Trainerize Clients' tab."""
    import gspread
    from google.oauth2.service_account import Credentials

    creds_path = os.path.join(os.path.dirname(__file__), GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_file(creds_path, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(GOOGLE_SHEET_ID)

    tab_name = "Trainerize Clients"
    try:
        ws = sheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=500, cols=15)

    headers = [
        "Trainerize ID", "First Name", "Last Name", "Email",
        "Profile Name", "Status", "Role", "Last Signed In",
        "Trial Status",
    ]

    rows = []
    for c in clients:
        rows.append([
            c.get("id", ""),
            c.get("firstName", ""),
            c.get("lastName", ""),
            c.get("email", ""),
            c.get("profileName", ""),
            c.get("status", ""),
            c.get("role", ""),
            c.get("latestSignedIn", ""),
            c.get("trialStatus", ""),
        ])

    ws.clear()
    ws.update([headers] + rows, "A1")
    ws.format("A1:I1", {"textFormat": {"bold": True}})

    print(f"\n✅ Exported {len(rows)} clients to '{tab_name}' tab in Google Sheets!")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Trainerize → Google Sheets: Compliance Extraction")
    print("=" * 60)

    # 1. Connect to Trainerize API
    print("\n📡 Step 1: Connecting to Trainerize API...")
    tz = TrainerizeClient(TZ_GROUP_ID, TZ_API_TOKEN)

    # 2. Fetch all active clients
    print("\n👥 Step 2: Fetching client list...")
    clients = tz.get_all_clients("activeClient")

    if not clients:
        print("❌ No clients found. Check your API credentials.")
        sys.exit(1)

    print(f"\n✅ Found {len(clients)} active clients!")
    for c in clients[:5]:
        print(f"   • {c['firstName']} {c['lastName']} ({c['status']})")
    if len(clients) > 5:
        print(f"   ... and {len(clients) - 5} more")

    # 3. Fetch compliance data for each client
    print(f"\n📊 Step 3: Fetching {COMPLIANCE_WEEKS}-week compliance data...")
    compliance_rows = build_compliance_rows(clients, tz)

    # 4. Export to Google Sheets
    print("\n📤 Step 4: Exporting to Google Sheets...")
    export_clients_to_sheets(clients)
    export_compliance_to_sheets(compliance_rows)

    print("\n" + "=" * 60)
    print("🎉 Done! Check your Google Sheet for the new tabs:")
    print("   • 'Trainerize Clients' — full client list")
    print("   • 'Trainerize Compliance' — weekly workout/habit compliance")
    print("=" * 60)


if __name__ == "__main__":
    main()
