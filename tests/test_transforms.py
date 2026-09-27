from src.extractors.renpho_api import convert_date, extract_weight_records
from src.processors.merge_master import build_master, fuzzy_match, build_word_set, parse_date_str, safe_weight
from src.utils.client_mapping import match_friend_to_client, parse_client_table

CLIENT_TABLE = [
    ["Client ID", "Client Name", "App ID"],
    ["1", "Alex Rivera", "alex.rivera"],
    ["2", "Jordan Lee", ""],
    ["x", "Bad Id", "bad"],
]


def test_client_table_and_friend_matching():
    maps = parse_client_table(CLIENT_TABLE)
    assert set(maps["name_map"]) == {"alex rivera", "jordan lee"}
    args = (maps["app_id_map"], maps["name_map"])
    assert match_friend_to_client("ALEX.RIVERA", *args) == (1, "Alex Rivera")
    assert match_friend_to_client("Jordan Lee", *args) == (2, "Jordan Lee")
    assert match_friend_to_client("Jordon Lee", *args) == (2, "Jordan Lee")
    assert match_friend_to_client("Somebody Else", *args) is None


def test_renpho_parsing():
    assert convert_date("2026-03-05 07:30:00") == "05/03/26"
    assert convert_date("2026-03-05") == "05/03/26"
    assert convert_date("N/A") == ""
    assert extract_weight_records(None) == []
    assert extract_weight_records({"list": [{"weight": 80.456, "localCreatedAt": "2026-03-05 07:30:00"}]}) == [
        ("80.46", "2026-03-05 07:30:00")
    ]
    assert extract_weight_records({"weight": 70, "date": "2026-01-01"}) == [("70", "2026-01-01")]


def test_merge_helpers():
    assert parse_date_str("05/03/26") == "2026-03-05"
    assert parse_date_str("05/03/2026") == "2026-03-05"
    assert safe_weight("12") == "" and safe_weight("81.5") == "81.5" and safe_weight("abc") == ""
    db = {"alex rivera": {}, "jordan lee": {}}
    idx = {k: build_word_set(k) for k in db}
    assert fuzzy_match("Alex  Rivera (UK)", db, idx) == "alex rivera"
    assert fuzzy_match("Unknown Person", db, idx) == "unknown person"


def test_build_master_prefers_trainerize_and_computes_progress():
    tables = {
        "Client Merged Data": [["Client Name", "Target Weight", "Start Date"], ["Alex Rivera", "80", "01/03/2026"],
                               ["Casey Morgan", "70", "01/03/2026"]],
        "Trainerize Daily Logs": [["Client Name", "Date", "Weight (kg)", "Coach", "Overall Compliance %"],
                                  ["Alex Rivera", "2026-03-02", "", "Coach A", "75"]],
        "Daily Record": [["Client ID", "Client Name", "Date", "Value"],
                         ["1", "Alex Rivera", "02/03/26", "90.0"],
                         ["1", "Alex Rivera", "09/03/26", "88.0"]],
    }
    master, ring = build_master(tables)
    header, rows = master[0], master[1:]
    col = {h: i for i, h in enumerate(header)}

    alex = [r for r in rows if r[col["Client Name"]] == "Alex Rivera"]
    assert [r[col["Date"]] for r in alex] == ["2026-03-02", "2026-03-09"]
    assert alex[0][col["Overall Compliance %"]] == "75"
    assert alex[0][col["Daily Weight (kg)"]] == "90.0"
    assert alex[0][col["Coach"]] == "Coach A"
    assert alex[0][col["Weight Loss"]] == "2.0"
    assert alex[0][col["Progress Percent"]] == "20.0"

    casey = [r for r in rows if r[col["Client Name"]] == "Casey Morgan"]
    assert len(casey) == 1 and casey[0][col["Date"]] == "2026-03-01"
    assert casey[0][col["Coach"]] == "Coach Unassigned"
    assert [r[1] for r in ring[1:]] == ["Alex Rivera", "Casey Morgan"]
