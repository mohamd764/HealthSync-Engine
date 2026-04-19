"""
Google Sheets integration for Renpho Master Data.
Reads "New Client Table" for ID mapping, writes to "Daily Record" tab.
"""
from __future__ import annotations

import os
from difflib import SequenceMatcher
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_client(credentials_path: str) -> gspread.Client:
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_sheet(credentials_path: str, sheet_id: str) -> gspread.Spreadsheet:
    return _get_client(credentials_path).open_by_key(sheet_id)


# ─── Read New Client Table ───────────────────────────────────────────────────

def read_client_table(
    sheet_id: str,
    credentials_path: str,
    tab_name: str = "New Client Table",
) -> dict:
    """
    Read the New Client Table and return two lookup dicts:
      app_id_map : {lowercase(App ID) -> (Client ID, Client Name)}
      name_map   : {lowercase(Client Name) -> (Client ID, Client Name)}
    """
    sheet = _get_sheet(credentials_path, sheet_id)
    ws = sheet.worksheet(tab_name)
    rows = ws.get_all_values()

    if not rows:
        return {"app_id_map": {}, "name_map": {}}

    # Headers: Client ID | Client Name | App ID | ...
    app_id_map: dict[str, tuple[int, str]] = {}
    name_map: dict[str, tuple[int, str]] = {}

    for row in rows[1:]:  # skip header
        if len(row) < 3 or not row[0].strip():
            continue
        try:
            client_id = int(row[0].strip())
        except ValueError:
            continue
        client_name = row[1].strip()
        app_id = row[2].strip()

        if app_id:
            app_id_map[app_id.lower()] = (client_id, client_name)
        if client_name:
            name_map[client_name.lower()] = (client_id, client_name)

    return {"app_id_map": app_id_map, "name_map": name_map}


# ─── Fuzzy Matching ──────────────────────────────────────────────────────────

def match_friend_to_client(
    friend_name: str,
    app_id_map: dict[str, tuple[int, str]],
    name_map: dict[str, tuple[int, str]],
    threshold: float = 0.75,
) -> tuple[int, str] | None:
    """
    Match a Renpho friend name to a client in the New Client Table.
    Priority:
      1. Exact match on App ID (column C)
      2. Exact match on Client Name (column B)
      3. Fuzzy match on App ID or Client Name (≥ threshold)
    Returns (client_id, client_name) or None if no match.
    """
    key = friend_name.strip().lower()

    # 1. Exact on App ID
    if key in app_id_map:
        return app_id_map[key]

    # 2. Exact on Client Name
    if key in name_map:
        return name_map[key]

    # 3. Fuzzy match
    best_score = 0.0
    best_match = None

    for app_id, val in app_id_map.items():
        score = SequenceMatcher(None, key, app_id).ratio()
        if score > best_score:
            best_score = score
            best_match = val

    for name, val in name_map.items():
        score = SequenceMatcher(None, key, name).ratio()
        if score > best_score:
            best_score = score
            best_match = val

    if best_score >= threshold:
        return best_match

    return None


# ─── Export to Daily Record ──────────────────────────────────────────────────

def export_to_daily_record(
    rows: list[dict[str, Any]],
    sheet_id: str,
    credentials_path: str,
    worksheet_name: str = "Daily Record",
) -> None:
    """
    Export rows to the Daily Record tab.
    Columns: Client ID | Client Name | Date (dd/mm/yy) | Value
    """
    sheet = _get_sheet(credentials_path, sheet_id)

    try:
        ws = sheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=worksheet_name, rows=2000, cols=10)

    headers = ["Client ID", "Client Name", "Date (dd/mm/yy)", "Value"]

    data = []
    for r in rows:
        data.append([
            r.get("client_id", 0),
            r.get("client_name", "Unknown"),
            r.get("date_recorded", ""),
            r.get("weight_kg", ""),
        ])

    ws.clear()
    ws.update([headers] + data, "A1")
    ws.format("A1:D1", {"textFormat": {"bold": True}})


# ─── Unmatched Report ────────────────────────────────────────────────────────

def write_unmatched_report(
    unmatched: list[str],
    sheet_id: str,
    credentials_path: str,
    tab_name: str = "Unmatched Names",
) -> None:
    """Create/update an 'Unmatched Names' tab listing friend names with no match."""
    if not unmatched:
        return

    sheet = _get_sheet(credentials_path, sheet_id)

    try:
        ws = sheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=200, cols=3)

    headers = ["Renpho Friend Name", "Status"]
    data = [[name, "No match found"] for name in sorted(set(unmatched))]

    ws.clear()
    ws.update([headers] + data, "A1")
    ws.format("A1:B1", {"textFormat": {"bold": True}})


# ─── Legacy wrapper (backward compat with old main.py) ──────────────────────

def export_to_sheet(
    rows: list[dict[str, Any]],
    sheet_id: str,
    credentials_path: str,
    worksheet_name: str = "Renpho Data",
) -> None:
    """Legacy 3-column export for the old main.py."""
    sheet = _get_sheet(credentials_path, sheet_id)
    try:
        ws = sheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=worksheet_name, rows=1000, cols=10)

    headers = ["Client Name", "Weight (Kg)", "Date of Record"]
    data = [[r["client_name"], r["weight_kg"], r["date_recorded"]] for r in rows]
    ws.clear()
    ws.update([headers] + data, "A1")
    ws.format("A1:C1", {"textFormat": {"bold": True}})


def export_from_env(rows: list[dict[str, Any]]) -> None:
    """Legacy wrapper used by old main.py."""
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    creds_path = os.getenv("GOOGLE_CREDENTIALS_JSON", "credentials.json")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set in .env")
    export_to_sheet(rows, sheet_id, creds_path)
