import csv
import os

import pytest

import run_pipeline
from src.config import ConfigError, Settings

SECRET_VARS = ("RENPHO_EMAIL", "RENPHO_PASSWORD", "TZ_GROUP_ID", "TZ_API_TOKEN", "GOOGLE_SHEET_ID")


@pytest.fixture(autouse=True)
def no_credentials(monkeypatch, tmp_path):
    for name in SECRET_VARS + ("GOOGLE_CREDENTIALS_JSON", "MERGER_SHEET_ID", "OUTPUT_DIR"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


def _read(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))


def test_dry_run_end_to_end(tmp_path):
    out = tmp_path / "out"
    assert run_pipeline.main(["--dry-run", "--output-dir", str(out)]) == 0

    master = _read(out / "sheets" / "looker_studio_master.csv")
    header = master[0]
    names = {row[header.index("Client Name")] for row in master[1:]}
    assert names == {"Alex Rivera", "Jordan Lee", "Sam Patel", "Taylor Brooks", "Casey Morgan"}
    assert any(row[header.index("Daily Weight (kg)")] for row in master[1:])

    assert _read(out / "sheets" / "unmatched_names.csv")[1][0] == "Guest Scale User"
    assert os.path.exists(out / "trainerize_daily_logs.csv")
    assert os.path.exists(out / "friends_weight_data.json")


def test_real_run_without_credentials_fails_cleanly(caplog):
    assert run_pipeline.main([]) == 2
    assert "missing environment variable TZ_API_TOKEN" in caplog.text


def test_validate_only_checks_selected_steps(tmp_path, monkeypatch):
    key = tmp_path / "key.json"
    key.write_text("{}")
    monkeypatch.setenv("GOOGLE_SHEET_ID", "sheet")
    monkeypatch.setenv("GOOGLE_CREDENTIALS_JSON", str(key))
    settings = Settings.from_env(dotenv=False)
    settings.validate(["merge"])
    with pytest.raises(ConfigError, match="RENPHO_EMAIL"):
        settings.validate(["renpho"])


def test_settings_repr_hides_secrets(monkeypatch):
    monkeypatch.setenv("TZ_API_TOKEN", "super-secret-value")
    monkeypatch.setenv("RENPHO_PASSWORD", "another-secret")
    text = repr(Settings.from_env(dotenv=False))
    assert "super-secret-value" not in text and "another-secret" not in text
