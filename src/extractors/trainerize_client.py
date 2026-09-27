"""Minimal client for the Trainerize REST API (v03), shared by the Trainerize extractors."""
from __future__ import annotations

import base64
import logging
import time

import requests

log = logging.getLogger(__name__)

API_BASE = "https://api.trainerize.com/v03"
REQUEST_TIMEOUT = 30
PAGE_SIZE = 50
DELAY_BETWEEN_REQUESTS = 0.5


class TrainerizeClient:
    """Authenticates with HTTP Basic auth (group ID : API token)."""

    def __init__(self, group_id: str, api_token: str, delay: float = DELAY_BETWEEN_REQUESTS):
        auth = base64.b64encode(f"{group_id}:{api_token}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {auth}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        self.delay = delay
        self._session = requests.Session()

    def _post(self, endpoint: str, payload: dict) -> dict:
        try:
            resp = self._session.post(
                f"{API_BASE}{endpoint}", headers=self.headers, json=payload, timeout=REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            log.warning("HTTP %s on %s", e.response.status_code if e.response is not None else "?", endpoint)
        except requests.exceptions.RequestException as e:
            log.warning("Request to %s failed: %s", endpoint, e.__class__.__name__)
        except ValueError:
            log.warning("Non-JSON response from %s", endpoint)
        return {}

    def get_all_clients(self, view: str = "activeClient") -> list[dict]:
        """Fetch every client in ``view``, following pagination."""
        clients: list[dict] = []
        start = 0
        while True:
            data = self._post(
                "/user/getClientList",
                {"view": view, "sort": "name", "start": start, "count": PAGE_SIZE, "verbose": True},
            )
            users = data.get("users", [])
            if not users:
                break
            clients.extend(users)
            log.debug("Fetched %d clients (total %d)", len(users), len(clients))
            if len(users) < PAGE_SIZE:
                break
            start += PAGE_SIZE
            time.sleep(self.delay)
        return clients

    def get_user_compliance(self, user_id: int, start_date: str, end_date: str) -> list[dict]:
        """Weekly workout / habit / cardio / nutrition compliance records for one client."""
        data = self._post(
            "/compliance/getUserCompliance",
            {"userID": user_id, "startDate": start_date, "endDate": end_date},
        )
        return data.get("compliances", [])


def client_full_name(client: dict) -> str:
    return f"{client.get('firstName', '')} {client.get('lastName', '')}".strip()


def client_coach_name(client: dict) -> str:
    trainer = (client.get("details") or {}).get("trainer") or {}
    if not isinstance(trainer, dict):
        return ""
    return f"{trainer.get('firstName', '')} {trainer.get('lastName', '')}".strip()
