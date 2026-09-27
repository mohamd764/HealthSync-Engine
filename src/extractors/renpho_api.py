"""
Renpho "friends" weight extractor.

A single coach-owned Renpho account adds every client as a friend; this step reads
each friend's weight history and writes it to the "Daily Record" tab, mapped to the
client IDs in the "New Client Table".

Renpho has no public API. Login and payload encryption come from the unofficial
``renpho-api`` package; the two friend endpoints below are undocumented app
endpoints and may change without notice:
  - /RenphoHealth/app/friend/friendsList
  - /RenphoHealth/app/friend/measure/trend
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from src.utils.client_mapping import CLIENT_TABLE_TAB, match_friend_to_client, parse_client_table
from src.utils.sheets_connector import SheetStore

log = logging.getLogger(__name__)

BASE_URL = "https://cloud.renpho.com"
DAILY_RECORD_TAB = "Daily Record"
UNMATCHED_TAB = "Unmatched Names"
DAILY_RECORD_HEADERS = ["Client ID", "Client Name", "Date (dd/mm/yy)", "Value"]
FRIENDS_PAGE_SIZE = 100


class RenphoFriendsAPI:
    """Thin wrapper over ``renpho.RenphoClient`` exposing the two friend endpoints."""

    def __init__(self, email: str, password: str):
        from renpho import RenphoClient

        self._client = RenphoClient(email=email, password=password)

    def login(self) -> None:
        self._client.login()

    def _headers(self) -> dict:
        return {
            "token": self._client.token,
            "userId": str(self._client.user_id),
            "appVersion": "7.6.4",
            "platform": "android",
            "systemVersion": "15",
            "languageCode": "en",
            "language": "en",
            "area": "GB",
            "userArea": "GB",
            "timeZone": "+0",
            "zoneId": "Europe/London",
            "Content-Type": "application/json;charset=UTF-8",
        }

    def _call(self, endpoint: str, payload: dict):
        from renpho.crypto import decrypt_response, encrypt_request

        resp = self._client._session.post(
            f"{BASE_URL}/RenphoHealth/app/{endpoint}",
            json=encrypt_request(payload),
            headers=self._headers(),
            timeout=30,
        )
        rdata = resp.json()
        if rdata.get("code") != 101:
            return None
        enc_data = rdata.get("data")
        if enc_data and isinstance(enc_data, str):
            return decrypt_response(enc_data)
        return enc_data

    def _friends_page(self, page_num: int | None) -> list[dict]:
        payload = {"userId": str(self._client.user_id)}
        if page_num is not None:
            payload.update(pageNum=page_num, pageSize=FRIENDS_PAGE_SIZE)
        data = self._call("friend/friendsList", payload)
        return (data.get("list") or data.get("data") or data.get("rows") or []) if data else []

    def list_friends(self) -> list[dict]:
        friends: list[dict] = []
        page_num = 1
        while True:
            page = self._friends_page(page_num)
            if not page and page_num == 1:
                page = self._friends_page(None)
            if not page:
                break
            friends.extend(page)
            if len(page) < FRIENDS_PAGE_SIZE:
                break
            page_num += 1
        return friends

    def get_weight_trend(self, friend_user_id) -> object:
        return self._call("friend/measure/trend", {
            "rhFriendId": str(friend_user_id),
            "timeZone": 0,
            "param": "weight",
            "sourceDataType": "",
            "timeType": "ALL",
            "pageNum": 1,
            "pageSize": 1000,
        })


def convert_date(date_str: str) -> str:
    """Convert 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' to 'dd/mm/yy'; return input unchanged if unparseable."""
    if not date_str or date_str == "N/A":
        return ""
    for fmt, length in (("%Y-%m-%d %H:%M:%S", 19), ("%Y-%m-%d", 10)):
        try:
            return datetime.strptime(date_str[:length], fmt).strftime("%d/%m/%y")
        except ValueError:
            continue
    return date_str


def extract_weight_records(weight_data) -> list[tuple[str, str]]:
    """Normalise the various trend-response shapes into ``[(weight_str, date_str), ...]``."""
    if weight_data is None:
        return []

    if isinstance(weight_data, list):
        records = weight_data
    elif isinstance(weight_data, dict):
        records = (
            weight_data.get("list")
            or weight_data.get("data")
            or weight_data.get("records")
            or weight_data.get("trendList")
            or weight_data.get("measureList")
            or next((v for v in weight_data.values() if isinstance(v, list) and v), None)
        )
        if not records:
            w = (weight_data.get("weight") or weight_data.get("bodyWeight")
                 or weight_data.get("lastWeight") or weight_data.get("value"))
            d = (weight_data.get("measureTime") or weight_data.get("date")
                 or weight_data.get("time") or weight_data.get("createTime"))
            return [(str(w), str(d) if d else "N/A")] if w else []
    else:
        return []

    results = []
    for rec in records:
        weight_val = rec.get("weight", "N/A")
        date_str = rec.get("localCreatedAt", "N/A")
        weight_str = f"{weight_val:.2f}" if isinstance(weight_val, (int, float)) else str(weight_val)
        results.append((weight_str, date_str[:19] if date_str else "N/A"))
    return results


def build_daily_rows(source, maps: dict) -> tuple[list[dict], list[str]]:
    """Fetch every friend's weight history and map it to client IDs. Returns (rows, unmatched_names)."""
    rows: list[dict] = []
    unmatched: list[str] = []

    for friend in source.list_friends():
        renpho_name = friend.get("accountName", "Unknown")
        match = match_friend_to_client(renpho_name, maps["app_id_map"], maps["name_map"])
        if match:
            client_id, client_name = match
        else:
            client_id, client_name = 0, renpho_name
            unmatched.append(renpho_name)

        try:
            weight_data = source.get_weight_trend(friend.get("userId", ""))
        except Exception as e:
            log.warning("Weight fetch failed for a friend: %s", e.__class__.__name__)
            weight_data = None

        records = extract_weight_records(weight_data) or [("N/A", "")]
        for weight_str, date_str in records:
            rows.append({
                "client_id": client_id,
                "client_name": client_name,
                "date_recorded": convert_date(date_str),
                "weight_kg": weight_str,
            })

    rows.sort(key=lambda r: ((r["client_name"] or "").strip(), _sortable_date(r["date_recorded"])))
    return rows, unmatched


def _sortable_date(ddmmyy: str) -> str:
    try:
        return datetime.strptime(ddmmyy, "%d/%m/%y").strftime("%Y-%m-%d")
    except ValueError:
        return ddmmyy or ""


def run(source, store: SheetStore, output_dir: str) -> int:
    maps = parse_client_table(store.read(CLIENT_TABLE_TAB))
    log.info("Loaded %d App IDs and %d client names from '%s'",
             len(maps["app_id_map"]), len(maps["name_map"]), CLIENT_TABLE_TAB)

    log.info("Logging in to Renpho...")
    source.login()
    rows, unmatched = build_daily_rows(source, maps)
    log.info("Fetched %d weight rows; %d unmatched friend(s)", len(rows), len(unmatched))

    store.write(DAILY_RECORD_TAB, [DAILY_RECORD_HEADERS] + [
        [r["client_id"], r["client_name"], r["date_recorded"], r["weight_kg"]] for r in rows
    ])
    if unmatched:
        store.write(UNMATCHED_TAB, [["Renpho Friend Name", "Status"]] +
                    [[name, "No match found"] for name in sorted(set(unmatched))])

    os.makedirs(output_dir, exist_ok=True)
    backup = os.path.join(output_dir, "friends_weight_data.json")
    with open(backup, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    log.info("Local backup: %s", backup)
    return len(rows)
