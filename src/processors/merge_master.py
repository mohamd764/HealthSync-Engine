"""
Build Looker Studio Master - V4 (All Sources Merged)
=====================================================
Merges daily records from ALL 3 sources:
  1. Trainerize Daily Logs  (88 clients, compliance + weight + waist)
  2. Daily Record - Main     (89 clients, Renpho weight data)
  3. Daily Record            (10 clients, older Renpho data)
Ensures EVERY active client appears in Looker Studio Master,
even those without any daily records (summary row only).
"""
import os, sys, re, time
import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

CRED_PATH = r"C:\Users\comp\Downloads\Trainerize_App\credentials.json"
GOOGLE_SHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE"
MERGER_SHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE"

# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_col(row, header, col_name, default=""):
    try:
        idx = header.index(col_name)
        return row[idx].strip() if len(row) > idx else default
    except ValueError:
        return default

def find_idx(headers, candidates):
    for i, h in enumerate(headers):
        h_clean = h.strip().lower()
        for c in candidates:
            if h_clean == c.lower(): return i
    return -1

def build_word_set(name):
    return set(name.lower().replace("-", " ").replace("(", "").replace(")", "").split())

def normalize_name(s):
    if not s: return ""
    s = s.strip().lower()
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    return " ".join(s.split())

def fuzzy_match(raw_name, static_db, word_index):
    k = raw_name.strip().lower()
    if k in static_db: return k
    # Substring match
    for db_k in static_db:
        if k in db_k or db_k in k: return db_k
    # Normalized match
    nk = normalize_name(k)
    for db_k in static_db:
        if normalize_name(db_k) == nk: return db_k
    # Word overlap match
    kw = build_word_set(k)
    noise = {"aka", "south", "canada", "uk", "jr", "sr", "disconnected", "disconnected?"}
    kw -= noise
    bk, bs, br = None, 0, 0.0
    for db_k, db_words in word_index.items():
        clean_db = db_words - noise
        if not clean_db or not kw: continue
        overlap = len(kw & clean_db)
        ratio = overlap / min(len(kw), len(clean_db))
        if overlap > bs or (overlap == bs and ratio > br):
            bs, br, bk = overlap, ratio, db_k
    if bs >= 2 or (bs == 1 and br >= 1.0): return bk
    return k

def safe_weight(val):
    """Return weight as string if valid (30-250 range), else empty string."""
    if not val: return ""
    try:
        v = float(val)
        return str(v) if 30.0 <= v <= 250.0 else ""
    except ValueError:
        return ""

def parse_date_str(s):
    """Normalize date strings to YYYY-MM-DD for sorting."""
    s = str(s).strip()
    if not s: return ""
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y", "%d/%m/%Y"]:
        try:
            return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s  # Return raw if no format matched

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Looker Studio Master Builder v4 (All Sources)")
    print("=" * 60)

    print("\n1. Authenticating...")
    creds = Credentials.from_service_account_file(
        CRED_PATH, 
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)

    # ─── 1. Load Static Client Data from Merger Sheet ─────────────────────
    print("\n2. Loading static client data from Merger Sheet...")
    merger_sh = gc.open_by_key(MERGER_SHEET_ID)
    merger_data = merger_sh.worksheet("Client Merged Data").get_all_values()
    m_h = merger_data[0]
    
    static_db = {}          # key (lower name) -> dict of static fields
    merger_word_index = {}   # key -> word set (for fuzzy matching)
    coach_map = {}           # key -> coach name
    
    for row in merger_data[1:]:
        raw_name = get_col(row, m_h, "Client Name")
        name = raw_name.lower().strip()
        if not name or len(name) < 3: continue
        merger_word_index[name] = build_word_set(name)
        static_db[name] = {
            "Original Name": raw_name,
            "Medical Notes": get_col(row, m_h, "Medication"),
            "Coach Notes": get_col(row, m_h, "Coaches Notes"),
            "Prog Risk Temp": get_col(row, m_h, "Prog Risk Temp"),
            "Overall Compliance Temp": get_col(row, m_h, "Overall Compliance Temp"),
            "Webinar 1": get_col(row, m_h, "Webinar 1"),
            "Webinar 2": get_col(row, m_h, "Webinar 2"),
            "Webinar 3": get_col(row, m_h, "Webinar 3"),
            "PWF PB": get_col(row, m_h, "PWF PB"),
            "Start Date": get_col(row, m_h, "Start Date"),
            "Current End Date": get_col(row, m_h, "Current End Date"),
            "Days in Programme": get_col(row, m_h, "Days in Programme"),
            "Days Remaining": get_col(row, m_h, "Days Remaining"),
            "Target Weight": get_col(row, m_h, "Target Weight"),
            "Coach": "",
        }
    print(f"   Loaded {len(static_db)} clients from Merger.")

    # ─── 1.5 Load Coach Assignments from Trainerize Clients ───────────────
    print("\n2b. Loading Coach assignments from Trainerize Clients...")
    try:
        tc_data = gc.open_by_key(GOOGLE_SHEET_ID).worksheet("Trainerize Clients").get_all_values()
        tc_h = tc_data[0]
        tc_fn_i = tc_h.index("First Name") if "First Name" in tc_h else -1
        tc_ln_i = tc_h.index("Last Name") if "Last Name" in tc_h else -1
        tc_co_i = tc_h.index("Assigned Coach") if "Assigned Coach" in tc_h else -1
        
        if tc_fn_i >= 0 and tc_co_i >= 0:
            for row in tc_data[1:]:
                fn = row[tc_fn_i].strip() if len(row) > tc_fn_i else ""
                ln = row[tc_ln_i].strip() if len(row) > tc_ln_i and tc_ln_i >= 0 else ""
                coach = row[tc_co_i].strip() if len(row) > tc_co_i else ""
                if not fn or not coach: continue
                
                full_name = f"{fn} {ln}".strip().lower()
                # Try to match to static_db
                matched = fuzzy_match(full_name, static_db, merger_word_index)
                coach_map[matched] = coach
                if matched in static_db:
                    static_db[matched]["Coach"] = coach
            
            print(f"   Loaded {len(coach_map)} coach assignments.")
        else:
            print("   Warning: Could not find coach columns in Trainerize Clients.")
    except Exception as e:
        print(f"   Warning loading Trainerize Clients: {e}")

    # ─── 2. Load Weight Analytics (computed summaries) ────────────────────
    print("\n3. Loading Weight Analytics (summaries)...")
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    try:
        wa_data = sh.worksheet("Weight Analytics").get_all_values()
        wa_h = [h.strip() for h in wa_data[0]]
        wa_matched = 0
        
        wa_sw_i = find_idx(wa_h, ["Starting Weight"])
        wa_lw_i = find_idx(wa_h, ["Latest Weight"])
        wa_wl_i = find_idx(wa_h, ["Weight Loss"])
        wa_ld_i = find_idx(wa_h, ["Largest Delta"])
        wa_tw_i = find_idx(wa_h, ["Target Weight"])
        wa_pp_i = find_idx(wa_h, ["Progress Percent"])
        wa_tl_i = find_idx(wa_h, ["Total To Lose"])

        for row in wa_data[1:]:
            name = get_col(row, wa_h, "Client Name").lower()
            if not name: continue
            
            matched_key = fuzzy_match(name, static_db, merger_word_index)
            if matched_key not in static_db:
                static_db[matched_key] = {"Original Name": name.title()}
                merger_word_index[matched_key] = build_word_set(matched_key)
            
            def get_safe_wa(idx):
                return row[idx].strip() if 0 <= idx < len(row) else ""

            wa_sw = get_safe_wa(wa_sw_i)
            if wa_sw and wa_sw != "0": 
                static_db[matched_key]["Starting Weight"] = wa_sw
            
            static_db[matched_key].update({
                "Latest Weight": get_safe_wa(wa_lw_i),
                "Weight Loss": get_safe_wa(wa_wl_i),
                "Largest Delta": get_safe_wa(wa_ld_i),
                "Progress Percent": get_safe_wa(wa_pp_i),
                "Total To Lose": get_safe_wa(wa_tl_i)
            })
            
            wa_tw = get_safe_wa(wa_tw_i)
            # Only use Weight Analytics target weight if the merged tab doesn't have one
            if wa_tw and wa_tw != "N/A":
                if not static_db[matched_key].get("Target Weight"):
                    static_db[matched_key]["Target Weight"] = wa_tw
            
            wa_matched += 1
        print(f"   Mapped {wa_matched} clients from Weight Analytics.")
    except Exception as e:
        print(f"   Warning mapping Weight Analytics: {e}")

    # ─── 3. Load ALL daily records from 3 sources ─────────────────────────
    # We'll build a unified record map: key -> list of {date, weight, waist, bf, 
    #   cardio, workouts, habits, compliance, coach, original_name}

    name_cache = {}
    def resolve_name(raw_name):
        k = raw_name.strip().lower()
        if k in name_cache: return name_cache[k]
        matched = fuzzy_match(k, static_db, merger_word_index)
        name_cache[k] = matched
        return matched

    # Unified daily records: key -> { "YYYY-MM-DD" -> {fields} }
    # Using date as dedup key per client
    unified = defaultdict(dict)  # client_key -> { date_str -> record_dict }

    # ─── Source A: Trainerize Daily Logs (richest: weight + compliance + coach)
    print("\n4. Loading Trainerize Daily Logs...")
    tz_data = sh.worksheet("Trainerize Daily Logs").get_all_values()
    tz_h = tz_data[0]
    tz_count = 0
    
    for row in tz_data[1:]:
        name = get_col(row, tz_h, "Client Name")
        if not name: continue
        rk = resolve_name(name)
        
        date_raw = get_col(row, tz_h, "Date")
        date_key = parse_date_str(date_raw)
        if not date_key: continue
        
        weight = safe_weight(get_col(row, tz_h, "Weight (kg)"))
        waist = get_col(row, tz_h, "Waist (cm)")
        bf = get_col(row, tz_h, "Body Fat %")
        coach = get_col(row, tz_h, "Coach")
        
        if waist in ("0", "0.0"): waist = ""
        if bf in ("0", "0.0"): bf = ""
        
        # Store coach mapping
        if coach and rk not in coach_map:
            coach_map[rk] = coach
            if rk in static_db:
                static_db[rk]["Coach"] = coach
        
        rec = {
            "date": date_key,
            "date_display": date_key,  # already YYYY-MM-DD
            "original_name": name,
            "weight": weight,
            "waist": waist,
            "body_fat": bf,
            "cardio": get_col(row, tz_h, "Cardio %"),
            "workouts": get_col(row, tz_h, "Workouts %", get_col(row, tz_h, "Workout %")),
            "habits": get_col(row, tz_h, "Habits %"),
            "compliance": get_col(row, tz_h, "Overall Compliance %"),
            "source": "trainerize"
        }
        
        # Trainerize has priority - overwrite if date already exists
        unified[rk][date_key] = rec
        tz_count += 1
    
    print(f"   {tz_count} records from {len(set(r for r in unified))} clients")

    # ─── Source B: Daily Record - Main (Renpho weight, 89 clients)
    print("\n5. Loading Daily Record - Main (Renpho)...")
    drm_data = sh.worksheet("Daily Record - Main").get_all_values()
    drm_h = drm_data[0]
    drm_count = 0
    drm_new_clients = 0
    
    for row in drm_data[1:]:
        name = row[1].strip() if len(row) > 1 else ""
        if not name or len(name) < 3: continue
        # Skip if name is just a number (Client ID leaked)
        if name.isdigit(): continue
        
        rk = resolve_name(name)
        
        date_raw = row[2].strip() if len(row) > 2 else ""
        date_key = parse_date_str(date_raw)
        if not date_key: continue
        
        weight_val = row[3].strip() if len(row) > 3 else ""
        weight = safe_weight(weight_val)
        
        # Only add if this date doesn't already have a Trainerize record
        if date_key not in unified[rk]:
            if rk not in static_db:
                static_db[rk] = {"Original Name": name}
                merger_word_index[rk] = build_word_set(rk)
                drm_new_clients += 1
            
            unified[rk][date_key] = {
                "date": date_key,
                "date_display": date_key,
                "original_name": name,
                "weight": weight,
                "waist": "",
                "body_fat": "",
                "cardio": "",
                "workouts": "",
                "habits": "",
                "compliance": "",
                "source": "renpho_main"
            }
            drm_count += 1
        elif weight and not unified[rk][date_key].get("weight"):
            # Fill in weight if Trainerize had the date but no weight
            unified[rk][date_key]["weight"] = weight
    
    print(f"   {drm_count} NEW records added from {drm_new_clients} new clients")

    # ─── Source C: Daily Record (older Renpho, 10 clients)
    print("\n6. Loading Daily Record (older Renpho)...")
    dr_data = sh.worksheet("Daily Record").get_all_values()
    dr_h = dr_data[0]
    dr_count = 0
    
    for row in dr_data[1:]:
        name = row[1].strip() if len(row) > 1 else ""
        if not name or len(name) < 3 or name.isdigit(): continue
        
        rk = resolve_name(name)
        
        date_raw = row[2].strip() if len(row) > 2 else ""
        date_key = parse_date_str(date_raw)
        if not date_key: continue
        
        weight_val = row[3].strip() if len(row) > 3 else ""
        weight = safe_weight(weight_val)
        
        if date_key not in unified[rk]:
            if rk not in static_db:
                static_db[rk] = {"Original Name": name}
                merger_word_index[rk] = build_word_set(rk)
            
            unified[rk][date_key] = {
                "date": date_key,
                "date_display": date_key,
                "original_name": name,
                "weight": weight,
                "waist": "",
                "body_fat": "",
                "cardio": "",
                "workouts": "",
                "habits": "",
                "compliance": "",
                "source": "renpho_old"
            }
            dr_count += 1
        elif weight and not unified[rk][date_key].get("weight"):
            unified[rk][date_key]["weight"] = weight
    
    print(f"   {dr_count} NEW records added")

    # ─── Summary stats ────────────────────────────────────────────────────
    total_clients = len(unified)
    total_records = sum(len(dates) for dates in unified.values())
    print(f"\n   TOTAL: {total_records} records across {total_clients} clients")

    # ─── Compute fallbacks for clients that have records but no WA data ──
    print("\n7. Computing weight analytics fallbacks...")
    for rk, date_records in unified.items():
        if rk not in static_db:
            static_db[rk] = {"Original Name": rk.title()}
            merger_word_index[rk] = build_word_set(rk)
        
        sd = static_db[rk]
        
        # Gather all valid weight readings
        readings = []
        for dk in sorted(date_records.keys()):
            rec = date_records[dk]
            w = rec.get("weight", "")
            if w:
                try: readings.append((dk, float(w)))
                except: pass
        
        if not readings: continue
        readings.sort(key=lambda x: x[0])
        
        # Fill Starting Weight if missing
        if not sd.get("Starting Weight") or sd["Starting Weight"] in ("0", ""):
            sd["Starting Weight"] = str(readings[0][1])
        
        # ALWAYS force Latest Weight dynamically if we have readings
        sd["Latest Weight"] = str(readings[-1][1])
        
        # Compute loss/delta/progress and force override static fields
        try:
            sw = float(sd.get("Starting Weight", 0) or 0)
            lw = float(sd.get("Latest Weight", 0) or 0)
            tw = float(sd.get("Target Weight", 0) or 0)
        except ValueError:
            continue
        
        if sw and lw:
            loss = round(sw - lw, 2)
            sd["Weight Loss"] = str(loss)
            
            all_w = [r[1] for r in readings]
            delta = round(sw - min(all_w), 2) if all_w else 0
            sd["Largest Delta"] = str(delta)
            
            if tw and sw > tw:
                total = round(sw - tw, 2)
                if total > 0:
                    sd["Total To Lose"] = str(total)
                    sd["Progress Percent"] = str(round((loss / total) * 100, 1))

    # ─── 4. Build Looker Studio Master Table ──────────────────────────────
    print("\n8. Building Master table...")
    
    MASTER_HEADERS = [
        "Date", "Client Name", "Coach", "Daily Weight (kg)", "Daily Waist (cm)", "Body Fat %",
        "Cardio %", "Workouts %", "Habits %", "Overall Compliance %",
        "Target Weight", "Starting Weight", "Latest Weight", "Weight Loss", 
        "Largest Delta", "Progress Percent", "Total To Lose", "Progress Display Text",
        "Medical Notes", "Coach Notes", "Prog Risk Temp", "Overall Compliance Temp",
        "Webinar 1", "Webinar 2", "Webinar 3", "PWF PB",
        "Start Date", "Current End Date", "Days in Programme", "Days Remaining"
    ]
    
    master_rows = [MASTER_HEADERS]
    clients_with_rows = set()
    
    for rk in sorted(unified.keys()):
        date_records = unified[rk]
        sd = static_db.get(rk, {})
        coach_fixed = sd.get("Coach", "") or coach_map.get(rk, "")
        if not coach_fixed:
            coach_fixed = "Coach Unassigned"
        
        display_name = sd.get("Original Name", rk.title())
        
        # Progress text
        loss_v = sd.get("Weight Loss", "0") or "0"
        total_v = sd.get("Total To Lose", "") or ""
        pdt = f"{loss_v} kg / {total_v} kg" if total_v and total_v != "0" else f"{loss_v} kg"
        
        # Static fields (same for every row of this client)
        static_cols = [
            sd.get("Target Weight", ""), sd.get("Starting Weight", ""), sd.get("Latest Weight", ""), sd.get("Weight Loss", ""),
            sd.get("Largest Delta", ""), sd.get("Progress Percent", ""), sd.get("Total To Lose", ""), pdt,
            sd.get("Medical Notes", ""), sd.get("Coach Notes", ""), sd.get("Prog Risk Temp", ""), sd.get("Overall Compliance Temp", ""),
            sd.get("Webinar 1", ""), sd.get("Webinar 2", ""), sd.get("Webinar 3", ""), sd.get("PWF PB", ""),
            sd.get("Start Date", ""), sd.get("Current End Date", ""), sd.get("Days in Programme", ""), sd.get("Days Remaining", "")
        ]
        
        for dk in sorted(date_records.keys()):
            rec = date_records[dk]
            master_rows.append([
                rec["date_display"],
                display_name,
                coach_fixed,
                rec.get("weight", ""),
                rec.get("waist", ""),
                rec.get("body_fat", ""),
                rec.get("cardio", ""),
                rec.get("workouts", ""),
                rec.get("habits", ""),
                rec.get("compliance", ""),
            ] + static_cols)
        
        clients_with_rows.add(rk)
    
    # ─── Add summary rows for clients WITHOUT any daily records ───────────
    # This ensures they still appear in Looker dropdown filters
    clients_no_records = set(static_db.keys()) - clients_with_rows
    print(f"   Adding {len(clients_no_records)} clients with no daily records as summary rows...")
    
    for rk in sorted(clients_no_records):
        sd = static_db[rk]
        display_name = sd.get("Original Name", rk.title())
        if not display_name or len(display_name) < 3: continue
        
        coach_fixed = sd.get("Coach", "") or coach_map.get(rk, "") or "Coach Unassigned"
        
        loss_v = sd.get("Weight Loss", "0") or "0"
        total_v = sd.get("Total To Lose", "") or ""
        pdt = f"{loss_v} kg / {total_v} kg" if total_v and total_v != "0" else f"{loss_v} kg"
        
        # Use Start Date as the single row's date, or empty
        row_date = sd.get("Start Date", "")
        if row_date:
            row_date = parse_date_str(row_date)
        
        master_rows.append([
            row_date,           # Date
            display_name,       # Client Name
            coach_fixed,        # Coach
            "",                 # Daily Weight (no data)
            "",                 # Waist
            "",                 # Body Fat
            "",                 # Cardio
            "",                 # Workouts
            "",                 # Habits
            "",                 # Compliance
            sd.get("Target Weight", ""), sd.get("Starting Weight", ""), sd.get("Latest Weight", ""), sd.get("Weight Loss", ""),
            sd.get("Largest Delta", ""), sd.get("Progress Percent", ""), sd.get("Total To Lose", ""), pdt,
            sd.get("Medical Notes", ""), sd.get("Coach Notes", ""), sd.get("Prog Risk Temp", ""), sd.get("Overall Compliance Temp", ""),
            sd.get("Webinar 1", ""), sd.get("Webinar 2", ""), sd.get("Webinar 3", ""), sd.get("PWF PB", ""),
            sd.get("Start Date", ""), sd.get("Current End Date", ""), sd.get("Days in Programme", ""), sd.get("Days Remaining", "")
        ])
    
    total_data_rows = len(master_rows) - 1
    final_clients = len(clients_with_rows) + len([rk for rk in clients_no_records 
                                                   if static_db.get(rk, {}).get("Original Name", "") and len(static_db.get(rk, {}).get("Original Name", "")) >= 3])
    print(f"   TOTAL: {total_data_rows} rows for {final_clients} clients")

    # ─── Upload ───────────────────────────────────────────────────────────
    print("\n9. Uploading Master worksheet...")
    ws = sh.worksheet("Looker Studio Master")
    
    # Resize sheet to fit all data
    needed_rows = len(master_rows) + 10
    needed_cols = len(MASTER_HEADERS)
    ws.resize(rows=needed_rows, cols=needed_cols)
    ws.clear()
    
    # Upload in batches if large
    BATCH = 4000
    for i in range(0, len(master_rows), BATCH):
        chunk = master_rows[i:i+BATCH]
        start_row = i + 1
        ws.update(values=chunk, range_name=f"A{start_row}")
        if i > 0:
            print(f"   Uploaded batch {i//BATCH + 1}...")
    
    print(f"   Uploaded {total_data_rows} rows to 'Looker Studio Master'")

    # ─── Progress Ring ────────────────────────────────────────────────────
    print("\n10. Updating Progress Ring worksheet...")
    ring_rows = [["Date", "Client Name", "Status", "Value", "Progress Display Text"]]
    for rk in sorted(static_db.keys()):
        sd = static_db[rk]
        display_name = sd.get("Original Name", rk.title())
        if not display_name or len(display_name) < 3: continue

        # Figure out the latest date for this client to satisfy Looker Studio filters
        latest_date = ""
        if rk in unified and unified[rk]:
            latest_date = max(unified[rk].keys())
        elif sd.get("Start Date"):
            latest_date = parse_date_str(sd.get("Start Date"))
        
        prog_v = sd.get("Progress Percent", "0") or "0"
        try: clamped_v = max(0.0, float(prog_v))
        except: clamped_v = 0.0
        
        loss_v = sd.get("Weight Loss", "0") or "0"
        total_v = sd.get("Total To Lose", "") or ""
        pdt = f"{loss_v} kg / {total_v} kg" if total_v and total_v != "0" else f"{loss_v} kg"
        ring_rows.append([latest_date, display_name, "Progress", str(clamped_v), pdt])
    
    try:
        pr = sh.worksheet("Progress Ring")
        pr.clear()
        pr.update(values=ring_rows, range_name="A1")
        print(f"   Updated {len(ring_rows)-1} progress entries.")
    except Exception as e:
        print(f"   Warning updating Progress Ring: {e}")

    print("\n" + "=" * 60)
    print("  DONE! Refresh Looker Studio Dashboard.")
    print("=" * 60)


if __name__ == "__main__":
    main()
