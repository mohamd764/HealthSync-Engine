"""
Renpho Friends Weight Tracker â†’ Daily Record
Fetches friend weight data from Renpho API and exports to the
"Daily Record" tab in the client's "Renpho Master Data" Google Sheet.

Endpoints discovered via MITM traffic analysis:
  - /RenphoHealth/app/friend/friendsList  (get friends list)
  - /RenphoHealth/app/friend/measure/trend (get weight data per friend)
"""

import json
import os
import sys
from datetime import datetime

os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

from dotenv import load_dotenv
from renpho import RenphoClient
from renpho.crypto import encrypt_request, decrypt_response
from sheets_export import (
    export_to_daily_record,
    match_friend_to_client,
    read_client_table,
    write_unmatched_report,
)

BASE_URL = "https://cloud.renpho.com"


def make_headers(client):
    return {
        "token": client.token,
        "userId": str(client.user_id),
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


def api_call(session, headers, endpoint, payload):
    url = f"{BASE_URL}/RenphoHealth/app/{endpoint}"
    body = encrypt_request(payload)
    resp = session.post(url, json=body, headers=headers, timeout=30)
    rdata = resp.json()
    if rdata.get("code") != 101:
        return None
    enc_data = rdata.get("data")
    if enc_data and isinstance(enc_data, str):
        return decrypt_response(enc_data)
    return enc_data


def get_friends_list(client):
    """Fetch all friends with pagination support."""
    headers = make_headers(client)
    all_friends = []
    page_num = 1
    page_size = 100

    def _fetch_page(use_pagination):
        if use_pagination:
            p = {"userId": str(client.user_id), "pageNum": page_num, "pageSize": page_size}
        else:
            p = {"userId": str(client.user_id)}
        data = api_call(client._session, headers, "friend/friendsList", p)
        return (data.get("list") or data.get("data") or data.get("rows") or []) if data else []

    while True:
        friends = _fetch_page(use_pagination=True)
        if not friends and page_num == 1:
            friends = _fetch_page(use_pagination=False)
        if not friends:
            break
        all_friends.extend(friends)
        if len(friends) < page_size:
            break
        page_num += 1
        print(f"  Fetched page {page_num - 1} ({len(friends)} friends), fetching more...")

    return all_friends


def get_friend_weight(client, friend_user_id):
    headers = make_headers(client)
    payload = {
        "rhFriendId": str(friend_user_id),
        "timeZone": 0,
        "param": "weight",
        "sourceDataType": "",
        "timeType": "ALL",
        "pageNum": 1,
        "pageSize": 1000,
    }
    return api_call(client._session, headers, "friend/measure/trend", payload)


def convert_date(date_str: str) -> str:
    """Convert 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' to 'dd/mm/yy' format."""
    if not date_str or date_str == "N/A":
        return ""
    try:
        # Try full datetime first
        dt = datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d/%m/%y")
    except ValueError:
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            return dt.strftime("%d/%m/%y")
        except ValueError:
            return date_str


def extract_weight_records(weight_data):
    """Extract a list of (weight, date_str) tuples from API response."""
    results = []
    if weight_data is None:
        return results

    if isinstance(weight_data, dict):
        records = (
            weight_data.get("list")
            or weight_data.get("data")
            or weight_data.get("records")
            or weight_data.get("trendList")
            or weight_data.get("measureList")
            or []
        )
        if not records:
            # Try to find any list inside the dict
            for key, val in weight_data.items():
                if isinstance(val, list) and val:
                    records = val
                    break
            if not records:
                # Try single value
                w = (
                    weight_data.get("weight")
                    or weight_data.get("bodyWeight")
                    or weight_data.get("lastWeight")
                    or weight_data.get("value")
                )
                d = (
                    weight_data.get("measureTime")
                    or weight_data.get("date")
                    or weight_data.get("time")
                    or weight_data.get("createTime")
                )
                if w:
                    results.append((str(w), str(d) if d else "N/A"))
                return results
    elif isinstance(weight_data, list):
        records = weight_data
    else:
        return results

    for rec in records:
        weight_val = rec.get("weight", "N/A")
        date_str = rec.get("localCreatedAt", "N/A")
        if isinstance(weight_val, (int, float)):
            weight_str = f"{weight_val:.2f}"
        else:
            weight_str = str(weight_val)
        results.append((weight_str, date_str[:19] if date_str else "N/A"))

    return results


def main():
    load_dotenv()
    email = os.getenv("RENPHO_EMAIL")
    password = os.getenv("RENPHO_PASSWORD")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    creds_path = os.getenv("GOOGLE_CREDENTIALS_JSON", "credentials.json")

    if not email or not password:
        print("Error: Set RENPHO_EMAIL and RENPHO_PASSWORD in .env")
        sys.exit(1)
    if not sheet_id:
        print("Error: Set GOOGLE_SHEET_ID in .env")
        sys.exit(1)

    # â”€â”€ Step 1: Read Client Table for ID mapping â”€â”€
    print("Reading 'New Client Table' for client mapping...")
    try:
        maps = read_client_table(sheet_id, creds_path)
        app_id_map = maps["app_id_map"]
        name_map = maps["name_map"]
        print(f"  Loaded {len(app_id_map)} App IDs and {len(name_map)} Client Names.\n")
    except Exception as e:
        print(f"Error reading client table: {e}")
        sys.exit(1)

    # â”€â”€ Step 2: Login to Renpho â”€â”€
    print(f"Logging in as {email}...")
    client = RenphoClient(email=email, password=password)
    client.login()
    print(f"Login OK. userId={client.user_id}\n")

    # â”€â”€ Step 3: Fetch friends list â”€â”€
    print("Fetching friends list...")
    friends = get_friends_list(client)
    print(f"Found {len(friends)} friends.\n")

    # â”€â”€ Step 4: Fetch weight data and match to clients â”€â”€
    all_rows = []
    unmatched_names = []

    for friend in friends:
        renpho_name = friend.get("accountName", "Unknown")
        friend_id = friend.get("userId", "")

        # Match to client
        match = match_friend_to_client(renpho_name, app_id_map, name_map)
        if match:
            client_id, client_name = match
            print(f"  âœ“ {renpho_name} â†’ Client #{client_id} ({client_name})")
        else:
            client_id, client_name = 0, renpho_name
            unmatched_names.append(renpho_name)
            print(f"  âœ— {renpho_name} â†’ NO MATCH (will use placeholder ID=0)")

        # Fetch weight history
        try:
            weight_data = get_friend_weight(client, friend_id)
        except Exception as e:
            print(f"    Error fetching weight: {e}")
            weight_data = None

        records = extract_weight_records(weight_data)

        if records:
            for weight_str, date_str in records:
                all_rows.append({
                    "client_id": client_id,
                    "client_name": client_name,
                    "date_recorded": convert_date(date_str),
                    "weight_kg": weight_str,
                })
            print(f"    {len(records)} weight records fetched.")
        else:
            all_rows.append({
                "client_id": client_id,
                "client_name": client_name,
                "date_recorded": "",
                "weight_kg": "N/A",
            })
            print(f"    No weight data available.")

    # â”€â”€ Step 5: Sort (group by client name, then date ascending) â”€â”€
    def _sort_key(row):
        name = (row["client_name"] or "").strip()
        d = (row["date_recorded"] or "").strip()
        return (name, d)

    all_rows.sort(key=_sort_key)

    # â”€â”€ Step 6: Print summary â”€â”€
    print(f"\n{'=' * 70}")
    print(f"  TOTAL ROWS TO EXPORT: {len(all_rows)}")
    print(f"  MATCHED FRIENDS: {len(friends) - len(unmatched_names)}")
    print(f"  UNMATCHED FRIENDS: {len(unmatched_names)}")
    print(f"{'=' * 70}")
    print(f"{'ID':<6} {'Client Name':<25} {'Weight':<12} {'Date':<15}")
    print(f"{'-' * 6} {'-' * 25} {'-' * 12} {'-' * 15}")
    for r in all_rows[:15]:
        print(f"{r['client_id']:<6} {r['client_name']:<25} {r['weight_kg']:<12} {r['date_recorded']:<15}")
    if len(all_rows) > 15:
        print(f"  ... and {len(all_rows) - 15} more rows")
    print(f"{'=' * 70}")

    if unmatched_names:
        print(f"\nUnmatched names: {', '.join(unmatched_names)}")

    # â”€â”€ Step 7: Export to Google Sheet â”€â”€
    print("\nExporting to 'Daily Record' tab...")
    try:
        export_to_daily_record(all_rows, sheet_id, creds_path)
        print("âœ“ Data exported to 'Daily Record' successfully.")
    except Exception as e:
        print(f"âœ— Failed to export: {e}")

    if unmatched_names:
        print("\nWriting unmatched names report...")
        try:
            write_unmatched_report(unmatched_names, sheet_id, creds_path)
            print("âœ“ Unmatched names report written.")
        except Exception as e:
            print(f"âœ— Failed to write unmatched report: {e}")

    # â”€â”€ Step 8: Save local backup â”€â”€
    output_file = os.path.join(os.path.dirname(__file__), "friends_weight_data.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)
    print(f"\nLocal backup saved: {output_file} ({len(all_rows)} rows)")


if __name__ == "__main__":
    main()

