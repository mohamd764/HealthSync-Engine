"""
Trainerize Daily Metrics Extraction Script v2
==============================================
Extracts daily historical body stats (Weight, Waist) and Cardio compliance
for all active clients. Saves to CSV locally and optionally exports to Google Sheets.

Usage: python trainerize_daily_metrics.py
"""
import base64
import csv
import json
import os
import sys
import time
from datetime import datetime, timedelta

import requests

sys.stdout.reconfigure(encoding='utf-8')

# ─── Configuration ───────────────────────────────────────────────────────────

TZ_GROUP_ID = "YOUR_TRAINERIZE_GROUP_ID"
TZ_API_TOKEN = "YOUR_API_TOKEN_HERE"
GOOGLE_SHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE"

API_BASE = "https://api.trainerize.com/v03"
HISTORY_DAYS = 90   # Body stats (per-day API calls - keep short)
COMPLIANCE_DAYS = 730  # Compliance (single API call per client - can be long)
REQUEST_TIMEOUT = 15
DELAY = 0.05

# Output CSV path (same folder as this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_OUTPUT = os.path.join(SCRIPT_DIR, "trainerize_daily_logs.csv")

# ─── Trainerize API ─────────────────────────────────────────────────────────

class TrainerizeClient:
    def __init__(self, group_id, api_token):
        auth = base64.b64encode(f"{group_id}:{api_token}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {auth}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _post(self, endpoint, payload):
        url = f"{API_BASE}{endpoint}"
        try:
            r = requests.post(url, headers=self.headers, json=payload, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            return None
        except Exception as e:
            print(f"    Warning: {endpoint} -> {e}")
            return None

    def get_all_clients(self):
        all_clients = []
        start = 0
        while True:
            payload = {
                "view": "activeClient",
                "sort": "name",
                "start": start,
                "count": 50,
                "verbose": True
            }
            data = self._post("/user/getClientList", payload)
            if not data:
                break
            users = data.get("users", [])
            if not users:
                break
            all_clients.extend(users)
            if len(users) < 50:
                break
            start += 50
            time.sleep(DELAY)
        return all_clients

    def get_body_stats(self, user_id, date_str):
        """Cracked endpoint: returns weight, body fat, waist etc."""
        payload = {
            "userID": user_id,
            "unitWeight": "kg",
            "unitBodystats": "cm",
            "date": date_str
        }
        return self._post("/bodyStats/get", payload)

    def get_user_compliance(self, user_id, start_date, end_date):
        """Returns weekly cardio/workout/habits compliance."""
        payload = {"userID": user_id, "startDate": start_date, "endDate": end_date}
        data = self._post("/compliance/getUserCompliance", payload)
        if data:
            return data.get("compliances", [])
        return []

# ─── Process Data ────────────────────────────────────────────────────────────

def process_client(client, tz, today, client_metadata):
    uid = client.get("id")
    name = f"{client.get('firstName', '')} {client.get('lastName', '')}".strip()
    
    meta = client_metadata.get(name.lower(), {})
    target_weight = meta.get("target_weight", "")
    
    # Extract Coach directly from Trainerize API (fallback to metadata if empty)
    trainer = client.get("details", {}).get("trainer", {})
    tz_coach = f"{trainer.get('firstName', '')} {trainer.get('lastName', '')}".strip() if isinstance(trainer, dict) else ""
    coach = tz_coach if tz_coach else meta.get("coach", "")
    
    rows = []

    # 1. Build compliance cache (weekly data for entire 2-year period - single API call)
    compliance_map = {}  # week_start -> {cardioCompleted, cardioScheduled, ...}
    start_date = (today - timedelta(days=COMPLIANCE_DAYS)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")

    compliances = tz.get_user_compliance(uid, start_date, end_date)
    for comp in compliances:
        ws = comp.get("startDate", "")
        compliance_map[ws] = comp
    time.sleep(DELAY)

    # 3. Fetch body stats in parallel using threads
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    dates_to_fetch = []
    for d in range(HISTORY_DAYS):
        target_date = today - timedelta(days=d)
        dates_to_fetch.append(target_date.strftime("%Y-%m-%d"))
    
    # Parallel fetch body stats
    body_stats_cache = {}  # date_str -> stats_response
    
    # --- DISABLED BODY STATS FETCH DUE TO RATE LIMITING ---
    # We now fetch ONLY weight from Renpho, so this is unnecessary API spam.
    body_stats_cache = {}
    
    # Now build rows: iterate over full COMPLIANCE_DAYS range
    data_found = 0
    for d in range(COMPLIANCE_DAYS):
        target_date = today - timedelta(days=d)
        date_str = target_date.strftime("%Y-%m-%d")

        weight = ""
        waist = ""
        body_fat = ""
        bmi = ""
        rhr = ""
        stats_source = ""

        # Body stats only available for recent days (from parallel fetch)
        stats = body_stats_cache.get(date_str)
        if stats:
            measures = stats.get("bodyMeasures", {})
            weight = measures.get("bodyWeight", "")
            waist = measures.get("waist", "")
            if waist and isinstance(waist, (int, float)) and 15 < waist < 50:
                waist = round(waist * 2.54, 2)
            body_fat = measures.get("bodyFatPercent", "")
            bmi = measures.get("bodyMassIndex", "")
            rhr = measures.get("restingHeartRate", "")
            stats_source = stats.get("from", "")

        # Weekly compliance for this date (available for full 730 days)
        week_start = target_date - timedelta(days=target_date.weekday())
        week_start_str = week_start.strftime("%Y-%m-%d")
        comp = compliance_map.get(week_start_str, {})
        cardio_done = comp.get("cardioCompleted", 0) or 0
        cardio_sched = comp.get("cardioScheduled", 0) or 0
        workout_done = comp.get("workoutCompleted", 0) or 0
        workout_sched = comp.get("workoutScheduled", 0) or 0
        habits_done = comp.get("habitsCompleted", 0) or 0
        habits_sched = comp.get("habitsScheduled", 0) or 0

        workout_pct = round(workout_done / workout_sched * 100) if workout_sched else ""
        habits_pct = round(habits_done / habits_sched * 100) if habits_sched else ""
        cardio_pct = round(cardio_done / cardio_sched * 100) if cardio_sched else ""
        
        total_done = cardio_done + workout_done + habits_done
        total_sched = cardio_sched + workout_sched + habits_sched
        overall_pct = round(total_done / total_sched * 100) if total_sched else ""

        has_data = weight or waist or body_fat or cardio_sched
        if has_data:
            rows.append([
                uid, name, coach, date_str,
                weight, target_weight, waist, body_fat, bmi, rhr, stats_source,
                cardio_done, cardio_sched, cardio_pct,
                workout_done, workout_sched, workout_pct,
                habits_done, habits_sched, habits_pct,
                overall_pct
            ])
            data_found += 1

    print(f"    -> {data_found} days with data (body stats: {len(body_stats_cache)}, compliance weeks: {len(compliance_map)})", flush=True)
    return rows

def get_client_metadata():
    """Fetch Target Weights from Weight Analytics and Coach names from New Client Table."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        cred_paths = [
            r"C:\Users\comp\Downloads\Trainerize_App\credentials.json",
            os.path.join(SCRIPT_DIR, "credentials.json"),
            os.getenv("GOOGLE_CREDENTIALS_JSON", ""),
        ]
        creds_file = next((p for p in cred_paths if p and os.path.exists(p)), None)
        if not creds_file: return {}

        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(GOOGLE_SHEET_ID)
        
        # 1. Fetch Coaches from New Client Table
        ws_new = sheet.worksheet("New Client Table")
        rows_new = ws_new.get_all_values()
        metadata = {}
        for row in rows_new[1:]:
            cname = row[1].strip().lower() if len(row) > 1 else ""
            if not cname: continue
            coach = row[13].strip() if len(row) > 13 else ""
            metadata[cname] = {"target_weight": "", "coach": coach}
            
        # 2. Fetch Authoritative Target Weights from Weight Analytics
        try:
            ws_wa = sheet.worksheet("Weight Analytics")
            rows_wa = ws_wa.get_all_values()
            # headers: 'Client Name', 'Starting Weight', 'Latest Weight', 'Target Weight', ...
            for row in rows_wa[1:]:
                cname = row[0].strip().lower() if len(row) > 0 else ""
                tw = row[3].strip() if len(row) > 3 and row[3].strip() != "N/A" else ""
                if cname:
                    if cname not in metadata:
                        metadata[cname] = {"target_weight": tw, "coach": ""}
                    else:
                        metadata[cname]["target_weight"] = tw
        except Exception as sheet_err:
            print(f"  WARNING: Could not fetch from Weight Analytics: {sheet_err}")

        return metadata
    except Exception as e:
        print(f"  WARNING: Could not fetch Client Metadata: {e}")
        return {}

# ─── CSV Export ──────────────────────────────────────────────────────────────

HEADERS = [
    "Trainerize ID", "Client Name", "Coach", "Date",
    "Weight (kg)", "Target Weight (kg)", "Waist (cm)", "Body Fat %", "BMI", "Resting HR", "Data Source",
    "Cardio Done", "Cardio Scheduled", "Cardio %",
    "Workouts Done", "Workouts Scheduled", "Workouts %",
    "Habits Done", "Habits Scheduled", "Habits %",
    "Overall Compliance %"
]

def save_csv(rows):
    rows.sort(key=lambda x: (x[1], x[2]), reverse=True)
    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(HEADERS)
        writer.writerows(rows)
    print(f"  Saved {len(rows)} rows to {CSV_OUTPUT}")

def export_to_sheets(rows):
    """Export to Google Sheets. Creates new sheet if existing one is inaccessible."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        # Try multiple credential paths
        cred_paths = [
            r"C:\Users\comp\Downloads\Trainerize_App\credentials.json",
            os.path.join(SCRIPT_DIR, "credentials.json"),
            os.getenv("GOOGLE_CREDENTIALS_JSON", ""),
        ]

        creds_file = None
        for p in cred_paths:
            if p and os.path.exists(p):
                creds_file = p
                break

        if not creds_file:
            print("  WARNING: No credentials.json found. Skipping Google Sheets export.")
            return False

        creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
        gc = gspread.authorize(creds)

        tab = "Trainerize Daily Logs"
        rows.sort(key=lambda x: (x[1], x[2]), reverse=True)
        sheet_data = [HEADERS] + rows

        # Strategy 1: Try existing sheet first
        try:
            sheet = gc.open_by_key(GOOGLE_SHEET_ID)
            try:
                ws = sheet.worksheet(tab)
            except gspread.WorksheetNotFound:
                ws = sheet.add_worksheet(title=tab, rows=5000, cols=20)
            ws.clear()
            ws.update(sheet_data, "A1")
            ws.format("A1:Q1", {"textFormat": {"bold": True}})
            print(f"  Exported {len(rows)} rows to existing sheet -> '{tab}'")
            return True
        except Exception as e1:
            print(f"  Existing sheet failed: {e1}")
            print(f"  Creating a NEW Google Sheet...")

        # Strategy 2: Create a brand new Google Sheet
        new_sheet = gc.create("Trainerize Daily Logs - Thrive With Age")
        ws = new_sheet.sheet1
        ws.update_title(tab)
        ws.resize(rows=len(rows) + 10, cols=20)
        ws.update(sheet_data, "A1")
        ws.format("A1:Q1", {"textFormat": {"bold": True}})

        # Share with the user and the client
        emails_to_share = [
            "contactYOUR_APP_NAME@gmail.com",  # Client email
            "ashiqd@gmail.com",                # Trainer email
        ]
        for email in emails_to_share:
            try:
                new_sheet.share(email, perm_type="user", role="writer", notify=True)
                print(f"  Shared with: {email}")
            except Exception:
                pass

        # Also make it accessible via link
        try:
            new_sheet.share("", perm_type="anyone", role="reader")
        except Exception:
            pass

        new_url = f"https://docs.google.com/spreadsheets/d/{new_sheet.id}/edit"
        print(f"\n  ✅ NEW Google Sheet created successfully!")
        print(f"  📎 URL: {new_url}")
        print(f"  Exported {len(rows)} rows to '{tab}'")

        # Save the new sheet URL for reference
        with open(os.path.join(SCRIPT_DIR, "NEW_SHEET_URL.txt"), "w") as f:
            f.write(f"New Trainerize Daily Logs Sheet\n")
            f.write(f"URL: {new_url}\n")
            f.write(f"Sheet ID: {new_sheet.id}\n")
            f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        return True

    except Exception as e:
        print(f"  WARNING: Google Sheets export failed: {e}")
        print(f"  Data is safely saved in CSV: {CSV_OUTPUT}")
        return False



# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Trainerize Daily Metrics Extraction v2")
    print("=" * 55)

    tz = TrainerizeClient(TZ_GROUP_ID, TZ_API_TOKEN)

    print("\n1. Fetching all clients...")
    all_clients = tz.get_all_clients()
    if not all_clients:
        print("ERROR: No clients found!")
        return
    print(f"   Found {len(all_clients)} clients")

    # ─── Client Filter Logic ───
    client_filter = os.getenv("CLIENT_FILTER", "").strip().lower()
    if client_filter:
        print(f"   Filtering for: '{client_filter}'")
        filtered = [c for c in all_clients if client_filter in f"{c.get('firstName', '')} {c.get('lastName', '')}".lower()]
        if not filtered:
            print(f"   Warning: No client found matching '{client_filter}'. Pulling all.")
            filtered = all_clients
    else:
        filtered = all_clients

    print(f"   Processing {len(filtered)} targeted clients.")

    print(f"\n2. Fetching Client Metadata from Master Sheet...")
    client_metadata = get_client_metadata()
    if client_metadata:
        print(f"   Fetched {len(client_metadata)} client records.")

    BATCH_SIZE = 10
    all_rows = []

    print(f"\n3. Extracting {HISTORY_DAYS} days for {len(filtered)} clients...", flush=True)
    today = datetime.now()
    
    for i, client in enumerate(filtered):
        name = f"{client.get('firstName','')} {client.get('lastName','')}".strip()
        print(f"  [{i+1}/{len(filtered)}] {name}...", flush=True)
        
        rows = process_client(client, tz, today, client_metadata)
        all_rows.extend(rows)

        # ─── New: Batch Save/Upload ───
        if (i + 1) % BATCH_SIZE == 0 or (i + 1) == len(filtered):
            print(f"\n  [Batch Progress] Saving {len(all_rows)} total rows so far...")
            save_csv(all_rows)
            export_to_sheets(all_rows)
            print(f"  [Batch] Success. Resuming next batch...\n")

    print("\n" + "=" * 55)
    print("  ALL DONE!")
    print(f"  Final CSV: {CSV_OUTPUT}")
    print("=" * 55)

if __name__ == "__main__":
    main()
