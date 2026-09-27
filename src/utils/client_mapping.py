"""Map Renpho "friend" names onto the coach-maintained "New Client Table"."""
from __future__ import annotations

from difflib import SequenceMatcher

CLIENT_TABLE_TAB = "New Client Table"

ClientRef = tuple[int, str]


def parse_client_table(rows: list[list[str]]) -> dict[str, dict[str, ClientRef]]:
    """
    Build lookup dicts from the New Client Table (columns: Client ID | Client Name | App ID | ...).

    Returns ``{"app_id_map": {app_id_lower: (id, name)}, "name_map": {name_lower: (id, name)}}``.
    """
    app_id_map: dict[str, ClientRef] = {}
    name_map: dict[str, ClientRef] = {}

    for row in rows[1:]:
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


def match_friend_to_client(
    friend_name: str,
    app_id_map: dict[str, ClientRef],
    name_map: dict[str, ClientRef],
    threshold: float = 0.75,
) -> ClientRef | None:
    """
    Match a Renpho friend name to a client.

    Priority: exact App ID, exact client name, then the best fuzzy match
    (difflib ratio >= ``threshold``) across both. Returns ``None`` if nothing matches.
    """
    key = friend_name.strip().lower()

    if key in app_id_map:
        return app_id_map[key]
    if key in name_map:
        return name_map[key]

    best_score = 0.0
    best_match = None
    for candidates in (app_id_map, name_map):
        for candidate, val in candidates.items():
            score = SequenceMatcher(None, key, candidate).ratio()
            if score > best_score:
                best_score, best_match = score, val

    return best_match if best_score >= threshold else None
