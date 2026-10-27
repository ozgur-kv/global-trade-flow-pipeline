"""Environment-backed configuration for UN Comtrade extraction."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _project_path(value: str | None, default: str) -> Path:
    path = Path(value or default)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _positive_float(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    try:
        value = default if raw_value is None else float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number.") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return value


def _positive_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    try:
        value = default if raw_value is None else int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer.") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")
    return value


@dataclass(frozen=True, slots=True)
class ComtradeSettings:
    """Runtime settings with conservative defaults for the free API tier."""

    api_key: str | None = None
    raw_dir: Path = PROJECT_ROOT / "data" / "raw"
    request_interval_seconds: float = 1.2
    timeout_seconds: float = 30.0
    daily_request_budget: int = 100
    budget_state_path: Path = (
        PROJECT_ROOT / "data" / "state" / "comtrade_request_budget.json"
    )

    def __post_init__(self) -> None:
        if self.request_interval_seconds <= 0:
            raise ValueError("request_interval_seconds must be greater than zero.")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero.")
        if self.daily_request_budget <= 0:
            raise ValueError("daily_request_budget must be greater than zero.")

    @classmethod
    def from_env(cls, env_file: Path | None = None) -> "ComtradeSettings":
        """Load settings without overriding values already in the environment."""

        load_dotenv(dotenv_path=env_file or PROJECT_ROOT / ".env", override=False)
        api_key = os.getenv("COMTRADE_API_KEY")
        if api_key is not None:
            api_key = api_key.strip() or None

        return cls(
            api_key=api_key,
            raw_dir=_project_path(os.getenv("COMTRADE_RAW_DIR"), "data/raw"),
            request_interval_seconds=_positive_float(
                "COMTRADE_REQUEST_INTERVAL_SECONDS", 1.2
            ),
            timeout_seconds=_positive_float("COMTRADE_TIMEOUT_SECONDS", 30.0),
            daily_request_budget=_positive_int(
                "COMTRADE_DAILY_REQUEST_BUDGET", 100
            ),
            budget_state_path=_project_path(
                os.getenv("COMTRADE_BUDGET_STATE_PATH"),
                "data/state/comtrade_request_budget.json",
            ),
        )
