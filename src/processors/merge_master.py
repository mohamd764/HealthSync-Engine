"""
Build the "Looker Studio Master" and "Progress Ring" tabs.

Merges per-client daily records from three sources, in priority order:
  1. Trainerize Daily Logs  (compliance; wins when a date exists in several sources)
  2. Daily Record - Main    (Renpho weights, legacy tab if present)
  3. Daily Record           (Renpho weights written by the Renpho step)

and joins them with coach-maintained static data ("Client Merged Data": target
weight, programme dates, notes) and computed summaries ("Weight Analytics").
Names are reconciled across sources with ``fuzzy_match``. Every known client gets
at least one row so it shows up in Looker Studio filters.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime

from src.utils.sheets_connector import SheetStore

log = logging.getLogger(__name__)

MERGED_DATA_TAB = "Client Merged Data"
TRAINERIZE_CLIENTS_TAB = "Trainerize Clients"
WEIGHT_ANALYTICS_TAB = "Weight Analytics"
TRAINERIZE_LOGS_TAB = "Trainerize Daily Logs"
DAILY_RECORD_MAIN_TAB = "Daily Record - Main"
DAILY_RECORD_TAB = "Daily Record"
MASTER_TAB = "Looker Studio Master"
PROGRESS_RING_TAB = "Progress Ring"

MIN_VALID_WEIGHT_KG = 30.0
MAX_VALID_WEIGHT_KG = 250.0
NAME_NOISE_WORDS = {"aka", "south", "canada", "uk", "jr", "sr", "disconnected", "disconnected?"}

# Output column -> column in "Client Merged Data"
STATIC_FIELDS = {
    "Medical Notes": "Medication",
    "Coach Notes": "Coaches Notes",
    "Prog Risk Temp": "Prog Risk Temp",
    "Overall Compliance Temp": "Overall Compliance Temp",
    "Webinar 1": "Webinar 1",
    "Webinar 2": "Webinar 2",
    "Webinar 3": "Webinar 3",
    "PWF PB": "PWF PB",
    "Start Date": "Start Date",
    "Current End Date": "Current End Date",
    "Days in Programme": "Days in Programme",
    "Days Remaining": "Days Remaining",
    "Target Weight": "Target Weight",
}

DAILY_HEADERS = [
    "Date", "Client Name", "Coach", "Daily Weight (kg)", "Daily Waist (cm)", "Body Fat %",
    "Cardio %", "Workouts %", "Habits %", "Overall Compliance %",
]
SUMMARY_HEADERS = [
    "Target Weight", "Starting Weight", "Latest Weight", "Weight Loss",
    "Largest Delta", "Progress Percent", "Total To Lose", "Progress Display Text",
    "Medical Notes", "Coach Notes", "Prog Risk Temp", "Overall Compliance Temp",
    "Webinar 1", "Webinar 2", "Webinar 3", "PWF PB",
    "Start Date", "Current End Date", "Days in Programme", "Days Remaining",
]
MASTER_HEADERS = DAILY_HEADERS + SUMMARY_HEADERS
RING_HEADERS = ["Date", "Client Name", "Status", "Value", "Progress Display Text"]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_col(row, header, col_name, default=""):
    try:
        idx = header.index(col_name)
    except ValueError:
        return default
    return row[idx].strip() if len(row) > idx else default


def find_idx(headers, candidates):
    wanted = {c.lower() for c in candidates}
    return next((i for i, h in enumerate(headers) if h.strip().lower() in wanted), -1)


def build_word_set(name):
    return set(name.lower().replace("-", " ").replace("(", "").replace(")", "").split())


def normalize_name(s):
    if not s:
        return ""
    s = re.sub(r"[^a-z0-9\s]", " ", s.strip().lower())
    return " ".join(s.split())


def fuzzy_match(raw_name, static_db, word_index):
    """
    Resolve ``raw_name`` to a key of ``static_db``: exact, substring, normalised,
    then best word overlap (>= 2 shared words, or 1 word covering the shorter name).
    Returns the lower-cased input if nothing matches.
    """
    k = raw_name.strip().lower()
    if k in static_db:
        return k
    for db_k in static_db:
        if k in db_k or db_k in k:
            return db_k
    nk = normalize_name(k)
    for db_k in static_db:
        if normalize_name(db_k) == nk:
            return db_k

    kw = build_word_set(k) - NAME_NOISE_WORDS
    best_key, best_overlap, best_ratio = None, 0, 0.0
    for db_k, db_words in word_index.items():
        clean_db = db_words - NAME_NOISE_WORDS
        if not clean_db or not kw:
            continue
        overlap = len(kw & clean_db)
        ratio = overlap / min(len(kw), len(clean_db))
        if overlap > best_overlap or (overlap == best_overlap and ratio > best_ratio):
            best_overlap, best_ratio, best_key = overlap, ratio, db_k
    if best_overlap >= 2 or (best_overlap == 1 and best_ratio >= 1.0):
        return best_key
    return k


def safe_weight(val):
    """Return the weight as a string if it is a plausible adult weight in kg, else ''."""
    if not val:
        return ""
    try:
        v = float(val)
    except ValueError:
        return ""
    return str(v) if MIN_VALID_WEIGHT_KG <= v <= MAX_VALID_WEIGHT_KG else ""


def parse_date_str(s):
    """Normalise a date string to YYYY-MM-DD; return it unchanged if no format matches."""
    s = str(s).strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s[:10], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s


def _progress_text(sd):
    loss_v = sd.get("Weight Loss", "0") or "0"
    total_v = sd.get("Total To Lose", "") or ""
    return f"{loss_v} kg / {total_v} kg" if total_v and total_v != "0" else f"{loss_v} kg"


def _summary_cols(sd):
    return [
        sd.get("Target Weight", ""), sd.get("Starting Weight", ""), sd.get("Latest Weight", ""),
        sd.get("Weight Loss", ""), sd.get("Largest Delta", ""), sd.get("Progress Percent", ""),
        sd.get("Total To Lose", ""), _progress_text(sd),
    ] + [sd.get(h, "") for h in SUMMARY_HEADERS[8:]]


def _empty_daily(date_key, name, weight, source):
    return {
        "date": date_key, "original_name": name, "weight": weight, "waist": "", "body_fat": "",
        "cardio": "", "workouts": "", "habits": "", "compliance": "", "source": source,
    }


# ─── Core merge (pure: tables in, rows out) ───────────────────────────────────

def build_master(tables: dict[str, list[list[str]]]) -> tuple[list[list], list[list]]:
    """
    ``tables`` maps tab name -> rows (header first); missing tabs may be omitted.
    Returns ``(master_rows, progress_ring_rows)``, each including a header row.
    """
    static_db: dict[str, dict] = {}
    word_index: dict[str, set] = {}
    coach_map: dict[str, str] = {}

    def ensure_client(key, display_name):
        if key not in static_db:
            static_db[key] = {"Original Name": display_name}
            word_index[key] = build_word_set(key)
            return True
        return False

    # 1. Static client data
    merger = tables.get(MERGED_DATA_TAB) or []
    if merger:
        m_h = merger[0]
        for row in merger[1:]:
            raw_name = get_col(row, m_h, "Client Name")
            name = raw_name.lower().strip()
            if len(name) < 3:
                continue
            word_index[name] = build_word_set(name)
            static_db[name] = {"Original Name": raw_name, "Coach": ""}
            static_db[name].update({out: get_col(row, m_h, src) for out, src in STATIC_FIELDS.items()})
    log.info("Loaded %d clients from '%s'", len(static_db), MERGED_DATA_TAB)

    # 2. Coach assignments
    tc = tables.get(TRAINERIZE_CLIENTS_TAB) or []
    if tc and "First Name" in tc[0] and "Assigned Coach" in tc[0]:
        tc_h = tc[0]
        for row in tc[1:]:
            full_name = f"{get_col(row, tc_h, 'First Name')} {get_col(row, tc_h, 'Last Name')}".strip().lower()
            coach = get_col(row, tc_h, "Assigned Coach")
            if not full_name or not coach:
                continue
            matched = fuzzy_match(full_name, static_db, word_index)
            coach_map[matched] = coach
            if matched in static_db:
                static_db[matched]["Coach"] = coach

    # 3. Weight Analytics summaries
    wa = tables.get(WEIGHT_ANALYTICS_TAB) or []
    if wa:
        wa_h = [h.strip() for h in wa[0]]
        idx = {
            key: find_idx(wa_h, [key])
            for key in ("Starting Weight", "Latest Weight", "Weight Loss", "Largest Delta",
                        "Target Weight", "Progress Percent", "Total To Lose")
        }
        for row in wa[1:]:
            name = get_col(row, wa_h, "Client Name").lower()
            if not name:
                continue
            key = fuzzy_match(name, static_db, word_index)
            ensure_client(key, name.title())
            cell = lambda col: row[idx[col]].strip() if 0 <= idx[col] < len(row) else ""

            if cell("Starting Weight") and cell("Starting Weight") != "0":
                static_db[key]["Starting Weight"] = cell("Starting Weight")
            for col in ("Latest Weight", "Weight Loss", "Largest Delta", "Progress Percent", "Total To Lose"):
                static_db[key][col] = cell(col)
            tw = cell("Target Weight")
            if tw and tw != "N/A" and not static_db[key].get("Target Weight"):
                static_db[key]["Target Weight"] = tw

    # 4. Daily records, deduplicated per client per date
    name_cache: dict[str, str] = {}

    def resolve_name(raw):
        k = raw.strip().lower()
        if k not in name_cache:
            name_cache[k] = fuzzy_match(k, static_db, word_index)
        return name_cache[k]

    unified: dict[str, dict[str, dict]] = defaultdict(dict)

    tz = tables.get(TRAINERIZE_LOGS_TAB) or []
    if tz:
        tz_h = tz[0]
        for row in tz[1:]:
            name = get_col(row, tz_h, "Client Name")
            date_key = parse_date_str(get_col(row, tz_h, "Date"))
            if not name or not date_key:
                continue
            rk = resolve_name(name)
            coach = get_col(row, tz_h, "Coach")
            if coach and rk not in coach_map:
                coach_map[rk] = coach
                if rk in static_db:
                    static_db[rk]["Coach"] = coach
            waist = get_col(row, tz_h, "Waist (cm)")
            bf = get_col(row, tz_h, "Body Fat %")
            unified[rk][date_key] = {
                "date": date_key,
                "original_name": name,
                "weight": safe_weight(get_col(row, tz_h, "Weight (kg)")),
                "waist": "" if waist in ("0", "0.0") else waist,
                "body_fat": "" if bf in ("0", "0.0") else bf,
                "cardio": get_col(row, tz_h, "Cardio %"),
                "workouts": get_col(row, tz_h, "Workouts %", get_col(row, tz_h, "Workout %")),
                "habits": get_col(row, tz_h, "Habits %"),
                "compliance": get_col(row, tz_h, "Overall Compliance %"),
                "source": "trainerize",
            }

    # Renpho tabs are positional: Client ID | Client Name | Date | Weight
    for tab, source in ((DAILY_RECORD_MAIN_TAB, "renpho_main"), (DAILY_RECORD_TAB, "renpho")):
        for row in (tables.get(tab) or [])[1:]:
            name = row[1].strip() if len(row) > 1 else ""
            if len(name) < 3 or name.isdigit():
                continue
            date_key = parse_date_str(row[2].strip() if len(row) > 2 else "")
            if not date_key:
                continue
            rk = resolve_name(name)
            weight = safe_weight(row[3].strip() if len(row) > 3 else "")
            if date_key not in unified[rk]:
                ensure_client(rk, name)
                unified[rk][date_key] = _empty_daily(date_key, name, weight, source)
            elif weight and not unified[rk][date_key].get("weight"):
                unified[rk][date_key]["weight"] = weight

    log.info("Merged %d daily records across %d clients",
             sum(len(v) for v in unified.values()), len(unified))

    # 5. Weight summaries computed from daily readings (override sheet values)
    for rk, date_records in unified.items():
        ensure_client(rk, rk.title())
        sd = static_db[rk]
        readings = []
        for dk in sorted(date_records):
            try:
                readings.append((dk, float(date_records[dk].get("weight") or "")))
            except ValueError:
                pass
        if not readings:
            continue

        if not sd.get("Starting Weight") or sd["Starting Weight"] == "0":
            sd["Starting Weight"] = str(readings[0][1])
        sd["Latest Weight"] = str(readings[-1][1])

        try:
            sw = float(sd.get("Starting Weight") or 0)
            lw = float(sd.get("Latest Weight") or 0)
            tw = float(sd.get("Target Weight") or 0)
        except ValueError:
            continue
        if sw and lw:
            loss = round(sw - lw, 2)
            sd["Weight Loss"] = str(loss)
            sd["Largest Delta"] = str(round(sw - min(w for _, w in readings), 2))
            if tw and sw > tw:
                total = round(sw - tw, 2)
                sd["Total To Lose"] = str(total)
                sd["Progress Percent"] = str(round(loss / total * 100, 1))

    # 6. Master table
    def coach_for(rk):
        return static_db.get(rk, {}).get("Coach") or coach_map.get(rk) or "Coach Unassigned"

    master_rows: list[list] = [MASTER_HEADERS]
    for rk in sorted(unified):
        sd = static_db.get(rk, {})
        display_name = sd.get("Original Name", rk.title())
        summary = _summary_cols(sd)
        for dk in sorted(unified[rk]):
            rec = unified[rk][dk]
            master_rows.append([
                rec["date"], display_name, coach_for(rk), rec["weight"], rec["waist"], rec["body_fat"],
                rec["cardio"], rec["workouts"], rec["habits"], rec["compliance"],
            ] + summary)

    for rk in sorted(set(static_db) - set(unified)):
        sd = static_db[rk]
        display_name = sd.get("Original Name", rk.title())
        if not display_name or len(display_name) < 3:
            continue
        master_rows.append(
            [parse_date_str(sd.get("Start Date", "")), display_name, coach_for(rk)] + [""] * 7 + _summary_cols(sd)
        )

    # 7. Progress ring (one row per client, dated at their latest record)
    ring_rows: list[list] = [RING_HEADERS]
    for rk in sorted(static_db):
        sd = static_db[rk]
        display_name = sd.get("Original Name", rk.title())
        if not display_name or len(display_name) < 3:
            continue
        latest_date = max(unified[rk]) if unified.get(rk) else parse_date_str(sd.get("Start Date", ""))
        try:
            progress = max(0.0, float(sd.get("Progress Percent") or 0))
        except ValueError:
            progress = 0.0
        ring_rows.append([latest_date, display_name, "Progress", str(progress), _progress_text(sd)])

    return master_rows, ring_rows


def run(store: SheetStore, merger_store: SheetStore | None = None) -> int:
    merger_store = merger_store or store
    tables = {MERGED_DATA_TAB: merger_store.read(MERGED_DATA_TAB)}
    for tab in (TRAINERIZE_CLIENTS_TAB, WEIGHT_ANALYTICS_TAB, TRAINERIZE_LOGS_TAB,
                DAILY_RECORD_MAIN_TAB, DAILY_RECORD_TAB):
        tables[tab] = store.read(tab)

    master_rows, ring_rows = build_master(tables)
    store.write(MASTER_TAB, master_rows)
    store.write(PROGRESS_RING_TAB, ring_rows)
    log.info("Wrote %d rows to '%s' and %d rows to '%s'",
             len(master_rows) - 1, MASTER_TAB, len(ring_rows) - 1, PROGRESS_RING_TAB)
    return len(master_rows) - 1
