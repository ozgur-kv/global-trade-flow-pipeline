"""Reliable extraction and raw snapshot storage for UN Comtrade."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Any
import warnings

import requests

from src.config import ComtradeSettings


PREVIEW_TRADE_BASE_URL = "https://comtradeapi.un.org/public/v1/preview"
FINAL_TRADE_BASE_URL = "https://comtradeapi.un.org/data/v1/get"
PREVIEW_METADATA_BASE_URL = "https://comtradeapi.un.org/public/v1/getMetadata"
FINAL_METADATA_BASE_URL = "https://comtradeapi.un.org/data/v1/getMetadata"

PREVIEW_MAX_RECORDS = 500
AUTHENTICATED_MAX_RECORDS = 100_000
MAX_ATTEMPTS = 4
MAX_RETRY_WAIT_SECONDS = 60.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
SNAPSHOT_METADATA_VERSION = 1
BUDGET_STATE_VERSION = 1


class ComtradeError(RuntimeError):
    """Raised when a Comtrade request or local extraction safeguard fails."""


@dataclass(frozen=True, slots=True)
class TradeQuery:
    """Parameters that define a UN Comtrade trade-data response."""

    type_code: str
    frequency_code: str
    classification_code: str
    period: str
    reporter_code: str
    partner_code: str
    commodity_code: str
    flow_code: str
    max_records: int | None = None
    partner2_code: str | None = None
    customs_code: str | None = None
    mode_of_transport_code: str | None = None
    breakdown_mode: str = "classic"
    include_descriptions: bool = True
    aggregate_by: str | None = None

    def __post_init__(self) -> None:
        required = {
            "type_code": self.type_code,
            "frequency_code": self.frequency_code,
            "classification_code": self.classification_code,
            "period": self.period,
            "reporter_code": self.reporter_code,
            "partner_code": self.partner_code,
            "commodity_code": self.commodity_code,
            "flow_code": self.flow_code,
        }
        for name, value in required.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string.")

        if self.type_code not in {"C", "S"}:
            raise ValueError("type_code must be 'C' or 'S'.")
        if self.frequency_code not in {"A", "M"}:
            raise ValueError("frequency_code must be 'A' or 'M'.")
        if self.breakdown_mode not in {"classic", "plus"}:
            raise ValueError("breakdown_mode must be 'classic' or 'plus'.")
        if self.max_records is not None and (
            not isinstance(self.max_records, int)
            or isinstance(self.max_records, bool)
            or self.max_records <= 0
        ):
            raise ValueError("max_records must be a positive integer.")

        for period in self.period.split(","):
            if self.frequency_code == "A":
                valid = len(period) == 4 and period.isdigit()
            else:
                valid = (
                    len(period) == 6
                    and period.isdigit()
                    and 1 <= int(period[-2:]) <= 12
                )
            if not valid:
                expected = "YYYY" if self.frequency_code == "A" else "YYYYMM"
                raise ValueError(f"period values must use {expected} format.")


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """A raw payload and the provenance needed by downstream consumers."""

    payload: dict[str, Any]
    payload_path: Path
    metadata_path: Path
    from_cache: bool
    record_count: int
    max_records: int
    possibly_truncated: bool
    warning_codes: tuple[str, ...]


class ComtradeClient:
    """Authenticated-first client with retries, pacing, quota, and caching."""

    def __init__(
        self,
        settings: ComtradeSettings,
        *,
        session: requests.Session | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        utc_now: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self._session = session or requests.Session()
        self._monotonic = monotonic
        self._sleep = sleep
        self._utc_now = utc_now or (lambda: datetime.now(timezone.utc))
        self._last_attempt_started: float | None = None

    @property
    def authenticated(self) -> bool:
        return bool(self.settings.api_key)

    def request_fingerprint(self, query: TradeQuery) -> str:
        """Return the credential-free identity of the effective trade request."""

        descriptor, _ = self._prepare_trade_request(query)
        return _fingerprint(descriptor)

    def fetch_trade_data(self, query: TradeQuery) -> dict[str, Any]:
        """Fetch and validate a trade payload without storing it."""

        descriptor, max_records = self._prepare_trade_request(query)
        payload = self._request_json(
            request_kind="trade",
            url=descriptor["endpoint"],
            params=descriptor["query_parameters"],
        )
        records = _validate_payload(payload, "trade", self.settings.api_key)
        possibly_truncated, warning_codes = _completeness(
            payload, records, max_records
        )
        _warn_if_truncated(possibly_truncated, warning_codes)
        return payload

    def fetch_trade_metadata(
        self,
        *,
        type_code: str,
        frequency_code: str,
        classification_code: str,
        period: str,
        reporter_code: str,
        show_history: bool = False,
    ) -> dict[str, Any]:
        """Fetch reporter metadata through the matching API mode."""

        for name, value in {
            "type_code": type_code,
            "frequency_code": frequency_code,
            "classification_code": classification_code,
            "period": period,
            "reporter_code": reporter_code,
        }.items():
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string.")

        base_url = (
            FINAL_METADATA_BASE_URL
            if self.authenticated
            else PREVIEW_METADATA_BASE_URL
        )
        url = f"{base_url}/{type_code}/{frequency_code}/{classification_code}"
        params = {
            "period": period,
            "reporterCode": reporter_code,
            "showHistory": str(show_history).lower(),
        }
        payload = self._request_json(
            request_kind="metadata", url=url, params=params
        )
        _validate_payload(payload, "metadata", self.settings.api_key)
        return payload

    def download_trade_data(
        self,
        query: TradeQuery,
        *,
        refresh: bool = False,
        raw_directory: Path | None = None,
    ) -> ExtractionResult:
        """Return a validated cache hit or fetch and save a new raw snapshot."""

        descriptor, max_records = self._prepare_trade_request(query)
        fingerprint = _fingerprint(descriptor)
        output_directory = raw_directory or self.settings.raw_dir

        if not refresh:
            cached = self._find_cached_snapshot(
                output_directory, fingerprint, descriptor, max_records
            )
            if cached is not None:
                _warn_if_truncated(
                    cached.possibly_truncated, cached.warning_codes
                )
                return cached

        payload = self._request_json(
            request_kind="trade",
            url=descriptor["endpoint"],
            params=descriptor["query_parameters"],
        )
        records = _validate_payload(payload, "trade", self.settings.api_key)
        possibly_truncated, warning_codes = _completeness(
            payload, records, max_records
        )
        result = self._write_snapshot(
            output_directory=output_directory,
            descriptor=descriptor,
            fingerprint=fingerprint,
            payload=payload,
            records=records,
            max_records=max_records,
            possibly_truncated=possibly_truncated,
            warning_codes=warning_codes,
        )
        _warn_if_truncated(possibly_truncated, warning_codes)
        return result

    def _prepare_trade_request(
        self, query: TradeQuery
    ) -> tuple[dict[str, Any], int]:
        max_records = query.max_records
        if max_records is None:
            max_records = (
                AUTHENTICATED_MAX_RECORDS
                if self.authenticated
                else PREVIEW_MAX_RECORDS
            )

        if self.authenticated:
            if max_records > AUTHENTICATED_MAX_RECORDS:
                raise ValueError(
                    "Authenticated free-tier requests support at most "
                    f"{AUTHENTICATED_MAX_RECORDS:,} records."
                )
            mode = "authenticated"
            base_url = FINAL_TRADE_BASE_URL
        else:
            if max_records > PREVIEW_MAX_RECORDS:
                raise ValueError(
                    f"Preview requests support at most {PREVIEW_MAX_RECORDS} records."
                )
            if "," in query.period or "," in query.commodity_code:
                raise ValueError(
                    "Preview requests support one period and one commodity per call."
                )
            mode = "preview"
            base_url = PREVIEW_TRADE_BASE_URL

        url = (
            f"{base_url}/{query.type_code}/{query.frequency_code}/"
            f"{query.classification_code}"
        )
        params: dict[str, Any] = {
            "period": query.period,
            "reporterCode": query.reporter_code,
            "partnerCode": query.partner_code,
            "cmdCode": query.commodity_code,
            "flowCode": query.flow_code,
            "maxRecords": max_records,
            "breakdownMode": query.breakdown_mode,
            "includeDesc": str(query.include_descriptions).lower(),
        }
        optional_params = {
            "partner2Code": query.partner2_code,
            "customsCode": query.customs_code,
            "motCode": query.mode_of_transport_code,
            "aggregateBy": query.aggregate_by,
        }
        params.update(
            {name: value for name, value in optional_params.items() if value is not None}
        )

        descriptor = {
            "request_kind": "trade",
            "mode": mode,
            "endpoint": url,
            "query_parameters": params,
        }
        return descriptor, max_records

    def _request_json(
        self, *, request_kind: str, url: str, params: dict[str, Any]
    ) -> dict[str, Any]:
        request_params = dict(params)
        if self.authenticated:
            request_params["subscription-key"] = self.settings.api_key

        for attempt_index in range(MAX_ATTEMPTS):
            self._wait_for_request_interval()
            self._consume_daily_budget()
            self._last_attempt_started = self._monotonic()

            try:
                response = self._session.get(
                    url,
                    params=request_params,
                    timeout=self.settings.timeout_seconds,
                )
            except (requests.ConnectionError, requests.Timeout):
                if attempt_index == MAX_ATTEMPTS - 1:
                    raise ComtradeError(
                        f"UN Comtrade {request_kind} request failed after "
                        f"{MAX_ATTEMPTS} attempts."
                    ) from None
                self._sleep(2**attempt_index)
                continue

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt_index == MAX_ATTEMPTS - 1:
                    raise ComtradeError(
                        f"UN Comtrade {request_kind} request failed with HTTP "
                        f"{response.status_code} after {MAX_ATTEMPTS} attempts."
                    ) from None
                retry_after = _retry_after_seconds(
                    response.headers.get("Retry-After"), self._utc_now()
                )
                self._sleep(
                    retry_after if retry_after is not None else 2**attempt_index
                )
                continue

            if response.status_code >= 400:
                raise ComtradeError(
                    f"UN Comtrade {request_kind} request failed with HTTP "
                    f"{response.status_code}."
                ) from None

            try:
                payload = response.json()
            except (ValueError, requests.JSONDecodeError):
                raise ComtradeError(
                    f"UN Comtrade {request_kind} response was not valid JSON."
                ) from None

            if not isinstance(payload, dict):
                raise ComtradeError(
                    f"UN Comtrade {request_kind} response must be a JSON object."
                )
            return payload

        raise AssertionError("Retry loop exited unexpectedly.")

    def _wait_for_request_interval(self) -> None:
        if self._last_attempt_started is None:
            return
        elapsed = self._monotonic() - self._last_attempt_started
        remaining = self.settings.request_interval_seconds - elapsed
        if remaining > 0:
            self._sleep(remaining)

    def _consume_daily_budget(self) -> None:
        path = self.settings.budget_state_path
        today = _as_utc(self._utc_now()).date().isoformat()
        attempts = 0

        if path.exists():
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                raise ComtradeError(
                    f"Local request-budget state is unreadable: {path}"
                ) from None

            valid_state = (
                isinstance(state, dict)
                and state.get("schema_version") == BUDGET_STATE_VERSION
                and isinstance(state.get("date_utc"), str)
                and isinstance(state.get("attempts_used"), int)
                and not isinstance(state.get("attempts_used"), bool)
                and state["attempts_used"] >= 0
                and isinstance(state.get("configured_limit"), int)
                and not isinstance(state.get("configured_limit"), bool)
                and state["configured_limit"] > 0
            )
            if not valid_state:
                raise ComtradeError(
                    f"Local request-budget state is invalid: {path}"
                )
            if state["date_utc"] == today:
                attempts = state["attempts_used"]

        if attempts >= self.settings.daily_request_budget:
            raise ComtradeError(
                "Local UN Comtrade daily request budget is exhausted "
                f"({attempts}/{self.settings.daily_request_budget} attempts)."
            )

        new_state = {
            "schema_version": BUDGET_STATE_VERSION,
            "date_utc": today,
            "attempts_used": attempts + 1,
            "configured_limit": self.settings.daily_request_budget,
        }
        _atomic_write_json(path, new_state)

    def _find_cached_snapshot(
        self,
        output_directory: Path,
        fingerprint: str,
        descriptor: dict[str, Any],
        max_records: int,
    ) -> ExtractionResult | None:
        if not output_directory.exists():
            return None

        pattern = f"comtrade_trade_{fingerprint}_*.json"
        candidates = sorted(output_directory.glob(pattern), reverse=True)
        for payload_path in candidates:
            if payload_path.name.endswith(".metadata.json"):
                continue
            metadata_path = _metadata_path(payload_path)
            result = self._validate_cached_snapshot(
                payload_path,
                metadata_path,
                descriptor,
                fingerprint,
                max_records,
            )
            if result is not None:
                return result
        return None

    def _validate_cached_snapshot(
        self,
        payload_path: Path,
        metadata_path: Path,
        descriptor: dict[str, Any],
        fingerprint: str,
        max_records: int,
    ) -> ExtractionResult | None:
        try:
            payload_bytes = payload_path.read_bytes()
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            payload = json.loads(payload_bytes.decode("utf-8"))
            records = _validate_payload(payload, "cached trade", None)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ComtradeError):
            return None

        possibly_truncated, warning_codes = _completeness(
            payload, records, max_records
        )
        expected_status = (
            "possibly_truncated" if possibly_truncated else "no_truncation_signal"
        )
        valid_metadata = (
            isinstance(metadata, dict)
            and metadata.get("schema_version") == SNAPSHOT_METADATA_VERSION
            and metadata.get("request_fingerprint") == fingerprint
            and metadata.get("request") == descriptor
            and isinstance(metadata.get("retrieved_at_utc"), str)
            and metadata.get("payload_sha256")
            == hashlib.sha256(payload_bytes).hexdigest()
            and metadata.get("record_count") == len(records)
            and metadata.get("max_records") == max_records
            and metadata.get("api_count") == payload.get("count")
            and metadata.get("possibly_truncated") is possibly_truncated
            and metadata.get("completeness_status") == expected_status
            and metadata.get("warning_codes") == list(warning_codes)
        )
        if not valid_metadata:
            return None

        return ExtractionResult(
            payload=payload,
            payload_path=payload_path,
            metadata_path=metadata_path,
            from_cache=True,
            record_count=len(records),
            max_records=max_records,
            possibly_truncated=possibly_truncated,
            warning_codes=warning_codes,
        )

    def _write_snapshot(
        self,
        *,
        output_directory: Path,
        descriptor: dict[str, Any],
        fingerprint: str,
        payload: dict[str, Any],
        records: list[Any],
        max_records: int,
        possibly_truncated: bool,
        warning_codes: tuple[str, ...],
    ) -> ExtractionResult:
        output_directory.mkdir(parents=True, exist_ok=True)
        timestamp = _as_utc(self._utc_now()).strftime("%Y%m%dT%H%M%S%fZ")
        stem = f"comtrade_trade_{fingerprint}_{timestamp}"
        payload_path = output_directory / f"{stem}.json"
        suffix = 1
        while payload_path.exists() or _metadata_path(payload_path).exists():
            payload_path = output_directory / f"{stem}_{suffix:02d}.json"
            suffix += 1
        metadata_path = _metadata_path(payload_path)

        payload_bytes = (
            json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
        )
        completeness_status = (
            "possibly_truncated" if possibly_truncated else "no_truncation_signal"
        )
        metadata = {
            "schema_version": SNAPSHOT_METADATA_VERSION,
            "request_fingerprint": fingerprint,
            "request": descriptor,
            "retrieved_at_utc": _as_utc(self._utc_now())
            .isoformat()
            .replace("+00:00", "Z"),
            "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "record_count": len(records),
            "api_count": payload.get("count"),
            "max_records": max_records,
            "possibly_truncated": possibly_truncated,
            "completeness_status": completeness_status,
            "warning_codes": list(warning_codes),
        }

        _atomic_write_bytes(payload_path, payload_bytes)
        _atomic_write_json(metadata_path, metadata)

        return ExtractionResult(
            payload=payload,
            payload_path=payload_path,
            metadata_path=metadata_path,
            from_cache=False,
            record_count=len(records),
            max_records=max_records,
            possibly_truncated=possibly_truncated,
            warning_codes=warning_codes,
        )


def _validate_payload(
    payload: dict[str, Any], request_kind: str, api_key: str | None
) -> list[Any]:
    if not isinstance(payload, dict):
        raise ComtradeError(
            f"UN Comtrade {request_kind} response must be a JSON object."
        )
    api_error = payload.get("error")
    if api_error:
        message = str(api_error)
        if api_key:
            message = message.replace(api_key, "[REDACTED]")
        raise ComtradeError(f"UN Comtrade {request_kind} error: {message}")
    records = payload.get("data")
    if not isinstance(records, list):
        raise ComtradeError(
            f"UN Comtrade {request_kind} response field 'data' must be a list."
        )
    api_count = payload.get("count")
    if api_count is not None and (
        not isinstance(api_count, int) or isinstance(api_count, bool) or api_count < 0
    ):
        raise ComtradeError(
            f"UN Comtrade {request_kind} response field 'count' must be "
            "a non-negative integer."
        )
    return records


def _completeness(
    payload: dict[str, Any], records: list[Any], max_records: int
) -> tuple[bool, tuple[str, ...]]:
    warning_codes: list[str] = []
    if len(records) >= max_records:
        warning_codes.append("MAX_RECORD_LIMIT_REACHED")
    api_count = payload.get("count")
    if isinstance(api_count, int) and api_count > len(records):
        warning_codes.append("API_COUNT_EXCEEDS_RETURNED_RECORDS")
    return bool(warning_codes), tuple(warning_codes)


def _warn_if_truncated(
    possibly_truncated: bool, warning_codes: tuple[str, ...]
) -> None:
    if possibly_truncated:
        warnings.warn(
            "UN Comtrade response may be incomplete: " + ", ".join(warning_codes),
            RuntimeWarning,
            stacklevel=3,
        )


def _fingerprint(descriptor: dict[str, Any]) -> str:
    canonical = json.dumps(
        descriptor, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _retry_after_seconds(value: str | None, now: datetime) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        seconds = (_as_utc(retry_at) - _as_utc(now)).total_seconds()
    return min(MAX_RETRY_WAIT_SECONDS, max(0.0, seconds))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _metadata_path(payload_path: Path) -> Path:
    return payload_path.with_name(f"{payload_path.stem}.metadata.json")


def _atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    content = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    _atomic_write_bytes(path, content)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
            temporary_path = Path(temporary_file.name)
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def _settings_with_overrides(
    subscription_key: str | None, timeout: float | None
) -> ComtradeSettings:
    settings = ComtradeSettings.from_env()
    changes: dict[str, Any] = {}
    if subscription_key is not None:
        changes["api_key"] = subscription_key
    if timeout is not None:
        changes["timeout_seconds"] = timeout
    return replace(settings, **changes) if changes else settings


def fetch_trade_data(
    *,
    type_code: str,
    frequency_code: str,
    classification_code: str,
    period: str,
    reporter_code: str,
    partner_code: str,
    commodity_code: str,
    flow_code: str,
    subscription_key: str | None = None,
    max_records: int | None = None,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Compatibility wrapper around :class:`ComtradeClient`."""

    settings = _settings_with_overrides(subscription_key, timeout)
    query = TradeQuery(
        type_code=type_code,
        frequency_code=frequency_code,
        classification_code=classification_code,
        period=period,
        reporter_code=reporter_code,
        partner_code=partner_code,
        commodity_code=commodity_code,
        flow_code=flow_code,
        max_records=max_records,
    )
    return ComtradeClient(settings).fetch_trade_data(query)


def save_raw_payload(
    payload: dict[str, Any],
    *,
    output_directory: Path,
    reporter_code: str,
    partner_code: str,
    flow_code: str,
    period: str,
) -> Path:
    """Compatibility helper for saving a payload without cache metadata."""

    output_directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    stem = (
        f"comtrade_reporter-{reporter_code}_partner-{partner_code}_"
        f"flow-{flow_code}_period-{period}_{timestamp}"
    )
    output_path = output_directory / f"{stem}.json"
    suffix = 1
    while output_path.exists():
        output_path = output_directory / f"{stem}_{suffix:02d}.json"
        suffix += 1
    content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    _atomic_write_bytes(output_path, content)
    return output_path


def fetch_trade_metadata(
    *,
    type_code: str,
    frequency_code: str,
    classification_code: str,
    period: str,
    reporter_code: str,
    subscription_key: str | None = None,
    show_history: bool = False,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Compatibility wrapper for metadata retrieval."""

    settings = _settings_with_overrides(subscription_key, timeout)
    return ComtradeClient(settings).fetch_trade_metadata(
        type_code=type_code,
        frequency_code=frequency_code,
        classification_code=classification_code,
        period=period,
        reporter_code=reporter_code,
        show_history=show_history,
    )
