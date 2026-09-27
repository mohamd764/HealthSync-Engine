"""Runtime configuration loaded from environment variables (and an optional .env file)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

REQUIRED_BY_STEP: dict[str, tuple[str, ...]] = {
    "renpho": ("RENPHO_EMAIL", "RENPHO_PASSWORD", "GOOGLE_SHEET_ID"),
    "trainerize-compliance": ("TZ_GROUP_ID", "TZ_API_TOKEN", "GOOGLE_SHEET_ID"),
    "trainerize-metrics": ("TZ_GROUP_ID", "TZ_API_TOKEN", "GOOGLE_SHEET_ID"),
    "merge": ("GOOGLE_SHEET_ID",),
}


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    renpho_email: str = ""
    renpho_password: str = field(default="", repr=False)
    tz_group_id: str = ""
    tz_api_token: str = field(default="", repr=False)
    google_sheet_id: str = ""
    merger_sheet_id: str = ""
    google_credentials_json: str = "credentials.json"
    client_filter: str = ""
    output_dir: str = "output"

    @classmethod
    def from_env(cls, dotenv: bool = True) -> "Settings":
        if dotenv:
            load_dotenv()
        get = lambda name, default="": os.getenv(name, default).strip()
        sheet_id = get("GOOGLE_SHEET_ID")
        return cls(
            renpho_email=get("RENPHO_EMAIL"),
            renpho_password=get("RENPHO_PASSWORD"),
            tz_group_id=get("TZ_GROUP_ID"),
            tz_api_token=get("TZ_API_TOKEN"),
            google_sheet_id=sheet_id,
            merger_sheet_id=get("MERGER_SHEET_ID") or sheet_id,
            google_credentials_json=get("GOOGLE_CREDENTIALS_JSON") or "credentials.json",
            client_filter=get("CLIENT_FILTER"),
            output_dir=get("OUTPUT_DIR") or "output",
        )

    def validate(self, steps: list[str]) -> None:
        """Raise ConfigError listing every missing variable needed by the selected steps."""
        values = {
            "RENPHO_EMAIL": self.renpho_email,
            "RENPHO_PASSWORD": self.renpho_password,
            "TZ_GROUP_ID": self.tz_group_id,
            "TZ_API_TOKEN": self.tz_api_token,
            "GOOGLE_SHEET_ID": self.google_sheet_id,
        }
        missing = sorted({name for step in steps for name in REQUIRED_BY_STEP[step] if not values[name]})
        problems = [f"missing environment variable {name}" for name in missing]
        if not os.path.isfile(self.google_credentials_json):
            problems.append(
                f"Google service-account key not found at '{self.google_credentials_json}' "
                "(set GOOGLE_CREDENTIALS_JSON)"
            )
        if problems:
            raise ConfigError("; ".join(problems))
