"""
Tabular storage backends.

Every pipeline step reads and writes named tabs through a ``SheetStore``:
``GoogleSheetStore`` talks to a real Google Sheet (what Looker Studio reads),
``LocalCsvStore`` writes one CSV per tab so the pipeline can run offline.
"""
from __future__ import annotations

import csv
import logging
import os
import re
from typing import Any

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
UPLOAD_BATCH_ROWS = 4000

Rows = list[list[Any]]


class SheetStore:
    def read(self, tab: str) -> list[list[str]]:
        """Return all rows of ``tab`` (header first), or ``[]`` if the tab does not exist."""
        raise NotImplementedError

    def write(self, tab: str, rows: Rows) -> None:
        """Replace the contents of ``tab`` with ``rows`` (header first), creating it if needed."""
        raise NotImplementedError


class GoogleSheetStore(SheetStore):
    def __init__(self, sheet_id: str, credentials_path: str):
        self.sheet_id = sheet_id
        self.credentials_path = credentials_path
        self._sheet = None

    def _spreadsheet(self):
        if self._sheet is None:
            import gspread
            from google.oauth2.service_account import Credentials

            creds = Credentials.from_service_account_file(self.credentials_path, scopes=SCOPES)
            self._sheet = gspread.authorize(creds).open_by_key(self.sheet_id)
        return self._sheet

    def read(self, tab: str) -> list[list[str]]:
        import gspread

        try:
            return self._spreadsheet().worksheet(tab).get_all_values()
        except gspread.WorksheetNotFound:
            log.warning("Tab '%s' not found; treating it as empty", tab)
            return []

    def write(self, tab: str, rows: Rows) -> None:
        import gspread
        from gspread.utils import rowcol_to_a1

        n_rows = max(len(rows), 1) + 10
        n_cols = max((len(r) for r in rows), default=1)
        sheet = self._spreadsheet()
        try:
            ws = sheet.worksheet(tab)
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet(title=tab, rows=n_rows, cols=n_cols)
        ws.clear()
        ws.resize(rows=n_rows, cols=n_cols)
        for start in range(0, len(rows), UPLOAD_BATCH_ROWS):
            ws.update(values=rows[start:start + UPLOAD_BATCH_ROWS], range_name=f"A{start + 1}")
        if rows:
            ws.format(f"A1:{rowcol_to_a1(1, n_cols)}", {"textFormat": {"bold": True}})


class LocalCsvStore(SheetStore):
    def __init__(self, directory: str):
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def path_for(self, tab: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", tab.lower()).strip("_")
        return os.path.join(self.directory, f"{slug}.csv")

    def read(self, tab: str) -> list[list[str]]:
        path = self.path_for(tab)
        if not os.path.exists(path):
            return []
        with open(path, newline="", encoding="utf-8") as f:
            return [row for row in csv.reader(f)]

    def write(self, tab: str, rows: Rows) -> None:
        with open(self.path_for(tab), "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(["" if v is None else v for v in row] for row in rows)
