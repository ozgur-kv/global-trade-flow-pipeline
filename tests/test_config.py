from pathlib import Path

import pytest

from src.config import ComtradeSettings, PROJECT_ROOT


ENVIRONMENT_VARIABLES = [
    "COMTRADE_API_KEY",
    "COMTRADE_RAW_DIR",
    "COMTRADE_REQUEST_INTERVAL_SECONDS",
    "COMTRADE_TIMEOUT_SECONDS",
    "COMTRADE_DAILY_REQUEST_BUDGET",
    "COMTRADE_BUDGET_STATE_PATH",
]


def clear_comtrade_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENVIRONMENT_VARIABLES:
        monkeypatch.delenv(name, raising=False)


def test_settings_defaults(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    clear_comtrade_environment(monkeypatch)

    settings = ComtradeSettings.from_env(tmp_path / "missing.env")

    assert settings.api_key is None
    assert settings.raw_dir == PROJECT_ROOT / "data" / "raw"
    assert settings.request_interval_seconds == 1.2
    assert settings.timeout_seconds == 30.0
    assert settings.daily_request_budget == 100
    assert settings.budget_state_path == (
        PROJECT_ROOT / "data" / "state" / "comtrade_request_budget.json"
    )


def test_settings_environment_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    clear_comtrade_environment(monkeypatch)
    monkeypatch.setenv("COMTRADE_API_KEY", "configured-key")
    monkeypatch.setenv("COMTRADE_RAW_DIR", "custom/raw")
    monkeypatch.setenv("COMTRADE_REQUEST_INTERVAL_SECONDS", "1.5")
    monkeypatch.setenv("COMTRADE_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("COMTRADE_DAILY_REQUEST_BUDGET", "80")
    monkeypatch.setenv("COMTRADE_BUDGET_STATE_PATH", "custom/state.json")

    settings = ComtradeSettings.from_env(tmp_path / "missing.env")

    assert settings.api_key == "configured-key"
    assert settings.raw_dir == PROJECT_ROOT / "custom" / "raw"
    assert settings.request_interval_seconds == 1.5
    assert settings.timeout_seconds == 45.0
    assert settings.daily_request_budget == 80
    assert settings.budget_state_path == PROJECT_ROOT / "custom" / "state.json"


def test_existing_environment_is_not_overridden(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    clear_comtrade_environment(monkeypatch)
    env_file = tmp_path / ".env"
    env_file.write_text("COMTRADE_API_KEY=file-key\n", encoding="utf-8")
    monkeypatch.setenv("COMTRADE_API_KEY", "process-key")

    settings = ComtradeSettings.from_env(env_file)

    assert settings.api_key == "process-key"


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("COMTRADE_REQUEST_INTERVAL_SECONDS", "0"),
        ("COMTRADE_TIMEOUT_SECONDS", "not-a-number"),
        ("COMTRADE_DAILY_REQUEST_BUDGET", "-1"),
    ],
)
def test_invalid_environment_values_fail_clearly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, name: str, value: str
) -> None:
    clear_comtrade_environment(monkeypatch)
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        ComtradeSettings.from_env(tmp_path / "missing.env")


def test_direct_settings_validation(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="daily_request_budget"):
        ComtradeSettings(
            raw_dir=tmp_path / "raw",
            budget_state_path=tmp_path / "state.json",
            daily_request_budget=0,
        )
