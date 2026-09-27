"""
Offline stand-ins for Renpho, Trainerize and the coach-maintained sheet tabs.

All people, IDs and measurements here are fictional and generated
deterministically; they exist only so the pipeline can run end to end
without credentials (``python run_pipeline.py --dry-run``) and in tests.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from src.utils.client_mapping import CLIENT_TABLE_TAB
from src.utils.sheets_connector import SheetStore

DEMO_WEEKS = 8

# (client_id, name, renpho_app_id, trainerize_id, coach, start_kg, target_kg, weekly_loss_kg)
DEMO_CLIENTS = [
    (1, "Alex Rivera", "alex.rivera", 9001, ("Demo", "Coach A"), 92.0, 82.0, 0.6),
    (2, "Jordan Lee", "", 9002, ("Demo", "Coach A"), 78.5, 72.0, 0.4),
    (3, "Sam Patel", "", 9003, ("Demo", "Coach B"), 105.2, 90.0, 0.8),
    (4, "Taylor Brooks", "tbrooks", 9004, ("Demo", "Coach B"), 68.0, 63.0, 0.2),
    (5, "Casey Morgan", "", 9005, ("Demo", "Coach A"), 84.0, 76.0, 0.0),
]

# Renpho account names intentionally differ from the client table to exercise matching:
# App ID match, exact name, typo (fuzzy), and one friend that matches nobody.
DEMO_RENPHO_FRIENDS = [
    ("alex.rivera", 1),
    ("Jordan Lee", 2),
    ("Sam Patil", 3),
    ("tbrooks", 4),
    ("Guest Scale User", None),
]


def _monday(d: datetime) -> datetime:
    return (d - timedelta(days=d.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def _client(client_id: int):
    return next(c for c in DEMO_CLIENTS if c[0] == client_id)


class FakeRenphoFriendsAPI:
    def __init__(self, today: datetime):
        self.today = today

    def login(self) -> None:
        pass

    def list_friends(self) -> list[dict]:
        return [{"accountName": name, "userId": f"r-{i}"} for i, (name, _) in enumerate(DEMO_RENPHO_FRIENDS)]

    def get_weight_trend(self, friend_user_id: str):
        _, client_id = DEMO_RENPHO_FRIENDS[int(friend_user_id.split("-")[1])]
        if client_id is None:
            return {"list": []}
        _, _, _, _, _, start_kg, _, weekly_loss = _client(client_id)
        records = []
        for week in range(DEMO_WEEKS):
            day = self.today - timedelta(weeks=DEMO_WEEKS - 1 - week)
            wobble = ((client_id * 7 + week * 3) % 5 - 2) * 0.1
            records.append({
                "weight": round(start_kg - weekly_loss * week + wobble, 2),
                "localCreatedAt": day.strftime("%Y-%m-%d 07:30:00"),
            })
        return {"list": records}


class FakeTrainerizeClient:
    delay = 0

    def __init__(self, today: datetime):
        self.today = today

    def get_all_clients(self, view: str = "activeClient") -> list[dict]:
        clients = []
        for client_id, name, _, tz_id, (coach_first, coach_last), *_ in DEMO_CLIENTS:
            first, last = name.split(" ", 1)
            clients.append({
                "id": tz_id,
                "firstName": first,
                "lastName": last,
                "email": f"{first.lower()}.{last.lower()}@example.com",
                "profileName": name,
                "status": "active",
                "role": "client",
                "latestSignedIn": self.today.strftime("%Y-%m-%d"),
                "trialStatus": "",
                "details": {"trainer": {"firstName": coach_first, "lastName": coach_last}},
            })
        return clients

    def get_user_compliance(self, user_id: int, start_date: str, end_date: str) -> list[dict]:
        start = _monday(datetime.strptime(start_date, "%Y-%m-%d"))
        end = datetime.strptime(end_date, "%Y-%m-%d")
        earliest = _monday(self.today) - timedelta(weeks=DEMO_WEEKS - 1)
        records = []
        week = max(start, earliest)
        while week <= end:
            n = (user_id + week.toordinal() // 7) % 4
            records.append({
                "startDate": week.strftime("%Y-%m-%d"),
                "endDate": (week + timedelta(days=6)).strftime("%Y-%m-%d"),
                "workoutScheduled": 3, "workoutCompleted": min(3, n + 1),
                "workoutCompliance": round(min(3, n + 1) / 3 * 100),
                "habitsScheduled": 7, "habitsCompleted": 3 + n,
                "habitsCompliance": round((3 + n) / 7 * 100),
                "cardioScheduled": 2, "cardioCompleted": n % 3,
                "nutritionCompleted": 4 + n, "nutritionCompliance": round((4 + n) / 7 * 100),
            })
            week += timedelta(weeks=1)
        return records


def seed_manual_tabs(store: SheetStore, today: datetime) -> None:
    """Write the tabs that coaches maintain by hand in the real sheet."""
    start_date = (_monday(today) - timedelta(weeks=DEMO_WEEKS)).strftime("%d/%m/%Y")
    end_date = (_monday(today) + timedelta(weeks=12)).strftime("%d/%m/%Y")

    client_table = [["Client ID", "Client Name", "App ID"] + [f"Col {i}" for i in range(4, 14)] + ["Coach"]]
    merged = [["Client Name", "Medication", "Coaches Notes", "Prog Risk Temp", "Overall Compliance Temp",
               "Webinar 1", "Webinar 2", "Webinar 3", "PWF PB", "Start Date", "Current End Date",
               "Days in Programme", "Days Remaining", "Target Weight"]]
    analytics = [["Client Name", "Starting Weight", "Latest Weight", "Target Weight"]]

    for client_id, name, app_id, _, coach, start_kg, target_kg, _ in DEMO_CLIENTS:
        coach_name = " ".join(coach)
        client_table.append([str(client_id), name, app_id] + [""] * 10 + [coach_name])
        merged.append([name, "", "Sample note", "Low", "", "Yes", "", "", "", start_date, end_date,
                       str(DEMO_WEEKS * 7), "84", str(target_kg)])
        analytics.append([name, str(start_kg), "", str(target_kg)])

    store.write(CLIENT_TABLE_TAB, client_table)
    store.write("Client Merged Data", merged)
    store.write("Weight Analytics", analytics)
