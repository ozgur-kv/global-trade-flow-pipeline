from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

import pytest
import requests

import src.extract as extract_module
from src.config import ComtradeSettings
from src.extract import (
    AUTHENTICATED_MAX_RECORDS,
    FINAL_METADATA_BASE_URL,
    FINAL_TRADE_BASE_URL,
    PREVIEW_MAX_RECORDS,
    PREVIEW_TRADE_BASE_URL,
    ComtradeClient,
    ComtradeError,
    TradeQuery,
)


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds


class FakeResponse:
    def __init__(
        self,
        payload: Any = None,
        *,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        json_error: Exception | None = None,
    ) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.json_error = json_error

    def json(self) -> Any:
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class FakeSession:
    def __init__(self, outcomes: list[Any] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self.outcomes:
            raise AssertionError("Unexpected HTTP request")
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def payload(records: list[dict[str, Any]] | None = None, count: int | None = None):
    data = [] if records is None else records
    return {
        "elapsedTime": "0.01 secs",
        "count": len(data) if count is None else count,
        "data": data,
        "error": "",
    }


@pytest.fixture
def query() -> TradeQuery:
    return TradeQuery(
        type_code="C",
        frequency_code="M",
        classification_code="HS",
        period="202301",
        reporter_code="276",
        partner_code="0",
        commodity_code="TOTAL",
        flow_code="X",
    )


def make_settings(
    tmp_path: Path,
    *,
    api_key: str | None = None,
    budget: int = 100,
    interval: float = 1.2,
) -> ComtradeSettings:
    return ComtradeSettings(
        api_key=api_key,
        raw_dir=tmp_path / "raw",
        request_interval_seconds=interval,
        timeout_seconds=30.0,
        daily_request_budget=budget,
        budget_state_path=tmp_path / "state" / "budget.json",
    )


def make_client(
    settings: ComtradeSettings,
    session: FakeSession,
    *,
    clock: FakeClock | None = None,
    now: datetime | None = None,
) -> tuple[ComtradeClient, FakeClock]:
    fake_clock = clock or FakeClock()
    current_time = now or datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
    return (
        ComtradeClient(
            settings,
            session=session,  # type: ignore[arg-type]
            monotonic=fake_clock.monotonic,
            sleep=fake_clock.sleep,
            utc_now=lambda: current_time,
        ),
        fake_clock,
    )


def test_authenticated_mode_is_primary_and_defaults_to_100k(
    tmp_path: Path, query: TradeQuery
) -> None:
    session = FakeSession([FakeResponse(payload())])
    client, _ = make_client(make_settings(tmp_path, api_key="secret"), session)

    client.fetch_trade_data(query)

    call = session.calls[0]
    assert call["url"].startswith(FINAL_TRADE_BASE_URL)
    assert call["params"]["maxRecords"] == AUTHENTICATED_MAX_RECORDS
    assert call["params"]["subscription-key"] == "secret"


def test_preview_mode_defaults_to_500(tmp_path: Path, query: TradeQuery) -> None:
    session = FakeSession([FakeResponse(payload())])
    client, _ = make_client(make_settings(tmp_path), session)

    client.fetch_trade_data(query)

    call = session.calls[0]
    assert call["url"].startswith(PREVIEW_TRADE_BASE_URL)
    assert call["params"]["maxRecords"] == PREVIEW_MAX_RECORDS
    assert "subscription-key" not in call["params"]


def test_mode_specific_record_limits(tmp_path: Path, query: TradeQuery) -> None:
    authenticated, _ = make_client(
        make_settings(tmp_path, api_key="secret"), FakeSession()
    )
    preview, _ = make_client(make_settings(tmp_path), FakeSession())

    with pytest.raises(ValueError, match="100,000"):
        authenticated.request_fingerprint(
            replace(query, max_records=AUTHENTICATED_MAX_RECORDS + 1)
        )
    with pytest.raises(ValueError, match="500"):
        preview.request_fingerprint(replace(query, max_records=501))

    with pytest.raises(ValueError, match="positive integer"):
        replace(query, max_records=True)


def test_authenticated_multiple_periods_and_flows(
    tmp_path: Path, query: TradeQuery
) -> None:
    session = FakeSession([FakeResponse(payload())])
    client, _ = make_client(make_settings(tmp_path, api_key="secret"), session)
    multi_query = replace(
        query, period="202301,202302,202303", flow_code="M,X", max_records=10_000
    )

    client.fetch_trade_data(multi_query)

    params = session.calls[0]["params"]
    assert params["period"] == "202301,202302,202303"
    assert params["flowCode"] == "M,X"


def test_preview_rejects_multiple_periods(tmp_path: Path, query: TradeQuery) -> None:
    client, _ = make_client(make_settings(tmp_path), FakeSession())
    with pytest.raises(ValueError, match="one period"):
        client.request_fingerprint(replace(query, period="202301,202302"))


def test_every_response_parameter_affects_fingerprint(
    tmp_path: Path, query: TradeQuery
) -> None:
    client, _ = make_client(make_settings(tmp_path, api_key="key-one"), FakeSession())
    base = replace(
        query,
        max_records=1_000,
        partner2_code="0",
        customs_code="C00",
        mode_of_transport_code="0",
        aggregate_by="cmdCode",
    )
    fingerprint = client.request_fingerprint(base)
    variations = [
        replace(base, type_code="S"),
        replace(base, frequency_code="A", period="2023"),
        replace(base, classification_code="H6"),
        replace(base, period="202302"),
        replace(base, reporter_code="792"),
        replace(base, partner_code="842"),
        replace(base, commodity_code="01"),
        replace(base, flow_code="M"),
        replace(base, max_records=999),
        replace(base, partner2_code="842"),
        replace(base, customs_code="C01"),
        replace(base, mode_of_transport_code="1"),
        replace(base, breakdown_mode="plus"),
        replace(base, include_descriptions=False),
        replace(base, aggregate_by="period"),
    ]
    assert all(client.request_fingerprint(item) != fingerprint for item in variations)

    other_key_client, _ = make_client(
        make_settings(tmp_path, api_key="key-two"), FakeSession()
    )
    assert other_key_client.request_fingerprint(base) == fingerprint
    assert "key-one" not in fingerprint


def test_retry_after_is_bounded_and_retry_counts_toward_budget(
    tmp_path: Path, query: TradeQuery
) -> None:
    session = FakeSession(
        [
            FakeResponse({}, status_code=429, headers={"Retry-After": "999"}),
            FakeResponse(payload()),
        ]
    )
    client, clock = make_client(
        make_settings(tmp_path, api_key="secret", interval=0.1), session
    )

    client.fetch_trade_data(query)

    assert clock.sleeps == [60.0]
    state = json.loads(client.settings.budget_state_path.read_text(encoding="utf-8"))
    assert state["attempts_used"] == 2


def test_http_date_retry_after_is_supported(
    tmp_path: Path, query: TradeQuery
) -> None:
    now = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
    retry_at = "Tue, 27 Oct 2026 12:00:05 GMT"
    session = FakeSession(
        [
            FakeResponse({}, status_code=503, headers={"Retry-After": retry_at}),
            FakeResponse(payload()),
        ]
    )
    client, clock = make_client(
        make_settings(tmp_path, interval=0.1), session, now=now
    )

    client.fetch_trade_data(query)

    assert clock.sleeps == [5.0]


def test_retry_fallbacks_are_deterministic(
    tmp_path: Path, query: TradeQuery
) -> None:
    session = FakeSession(
        [
            FakeResponse({}, status_code=500),
            requests.Timeout(),
            FakeResponse({}, status_code=502),
            FakeResponse(payload()),
        ]
    )
    client, clock = make_client(
        make_settings(tmp_path, api_key="secret", interval=0.1), session
    )

    client.fetch_trade_data(query)

    assert clock.sleeps == [1, 2, 4]
    state = json.loads(client.settings.budget_state_path.read_text(encoding="utf-8"))
    assert state["attempts_used"] == 4


def test_sequential_requests_are_paced(tmp_path: Path, query: TradeQuery) -> None:
    session = FakeSession([FakeResponse(payload()), FakeResponse(payload())])
    client, clock = make_client(make_settings(tmp_path), session)

    client.fetch_trade_data(query)
    client.fetch_trade_data(query)

    assert clock.sleeps == [1.2]


def test_non_retryable_error_is_sanitized(
    tmp_path: Path, query: TradeQuery
) -> None:
    secret = "do-not-expose"
    session = FakeSession([FakeResponse({}, status_code=401)])
    client, _ = make_client(make_settings(tmp_path, api_key=secret), session)

    with pytest.raises(ComtradeError) as captured:
        client.fetch_trade_data(query)

    assert "401" in str(captured.value)
    assert secret not in str(captured.value)
    assert len(session.calls) == 1


def test_api_error_redacts_key(tmp_path: Path, query: TradeQuery) -> None:
    secret = "private-key"
    session = FakeSession(
        [FakeResponse({"count": 0, "data": [], "error": f"bad {secret}"})]
    )
    client, _ = make_client(make_settings(tmp_path, api_key=secret), session)

    with pytest.raises(ComtradeError) as captured:
        client.fetch_trade_data(query)

    assert secret not in str(captured.value)
    assert "[REDACTED]" in str(captured.value)


@pytest.mark.parametrize(
    "bad_payload",
    [
        [],
        {"count": 0, "error": ""},
        {"count": "zero", "data": [], "error": ""},
    ],
)
def test_malformed_payloads_fail(
    tmp_path: Path, query: TradeQuery, bad_payload: Any
) -> None:
    session = FakeSession([FakeResponse(bad_payload)])
    client, _ = make_client(make_settings(tmp_path), session)

    with pytest.raises(ComtradeError):
        client.fetch_trade_data(query)


def test_metadata_uses_authenticated_endpoint_and_budget(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse(payload([{"notes": []}]))])
    client, _ = make_client(make_settings(tmp_path, api_key="secret"), session)

    client.fetch_trade_metadata(
        type_code="C",
        frequency_code="M",
        classification_code="HS",
        period="202301",
        reporter_code="276",
    )

    assert session.calls[0]["url"].startswith(FINAL_METADATA_BASE_URL)
    state = json.loads(client.settings.budget_state_path.read_text(encoding="utf-8"))
    assert state["attempts_used"] == 1


def test_metadata_payload_is_validated(tmp_path: Path) -> None:
    session = FakeSession([FakeResponse({"count": 0, "error": ""})])
    client, _ = make_client(make_settings(tmp_path, api_key="secret"), session)

    with pytest.raises(ComtradeError, match="field 'data'"):
        client.fetch_trade_metadata(
            type_code="C",
            frequency_code="M",
            classification_code="HS",
            period="202301",
            reporter_code="276",
        )


def test_budget_persists_across_clients_and_blocks_before_http(
    tmp_path: Path, query: TradeQuery
) -> None:
    settings = make_settings(tmp_path, api_key="secret", budget=2)
    first_session = FakeSession([FakeResponse(payload())])
    first, _ = make_client(settings, first_session)
    first.fetch_trade_data(query)

    second_session = FakeSession([FakeResponse(payload())])
    second, _ = make_client(settings, second_session)
    second.fetch_trade_data(query)

    blocked_session = FakeSession([FakeResponse(payload())])
    blocked, _ = make_client(settings, blocked_session)
    with pytest.raises(ComtradeError, match="budget is exhausted"):
        blocked.fetch_trade_data(query)
    assert blocked_session.calls == []


def test_budget_resets_on_new_utc_date(tmp_path: Path, query: TradeQuery) -> None:
    settings = make_settings(tmp_path, budget=1)
    day_one = datetime(2026, 10, 27, 23, 59, tzinfo=timezone.utc)
    client_one, _ = make_client(
        settings, FakeSession([FakeResponse(payload())]), now=day_one
    )
    client_one.fetch_trade_data(query)

    day_two = day_one + timedelta(minutes=2)
    session_two = FakeSession([FakeResponse(payload())])
    client_two, _ = make_client(settings, session_two, now=day_two)
    client_two.fetch_trade_data(query)

    state = json.loads(settings.budget_state_path.read_text(encoding="utf-8"))
    assert state["date_utc"] == "2026-10-28"
    assert state["attempts_used"] == 1


def test_corrupt_budget_state_fails_closed(tmp_path: Path, query: TradeQuery) -> None:
    settings = make_settings(tmp_path)
    settings.budget_state_path.parent.mkdir(parents=True)
    settings.budget_state_path.write_text("not-json", encoding="utf-8")
    session = FakeSession([FakeResponse(payload())])
    client, _ = make_client(settings, session)

    with pytest.raises(ComtradeError, match="budget state is unreadable"):
        client.fetch_trade_data(query)
    assert session.calls == []


def test_snapshot_metadata_flags_truncation_and_cache_uses_no_budget(
    tmp_path: Path, query: TradeQuery
) -> None:
    limited_query = replace(query, max_records=2)
    response_payload = payload([{"row": 1}, {"row": 2}])
    session = FakeSession([FakeResponse(response_payload)])
    client, _ = make_client(make_settings(tmp_path, api_key="secret"), session)

    with pytest.warns(RuntimeWarning, match="MAX_RECORD_LIMIT_REACHED"):
        first = client.download_trade_data(limited_query)
    with pytest.warns(RuntimeWarning, match="MAX_RECORD_LIMIT_REACHED"):
        cached = client.download_trade_data(limited_query)

    assert first.from_cache is False
    assert cached.from_cache is True
    assert cached.possibly_truncated is True
    assert cached.warning_codes == ("MAX_RECORD_LIMIT_REACHED",)
    assert len(session.calls) == 1

    metadata = json.loads(first.metadata_path.read_text(encoding="utf-8"))
    assert metadata["possibly_truncated"] is True
    assert metadata["completeness_status"] == "possibly_truncated"
    assert metadata["warning_codes"] == ["MAX_RECORD_LIMIT_REACHED"]
    assert "subscription-key" not in json.dumps(metadata)

    state = json.loads(client.settings.budget_state_path.read_text(encoding="utf-8"))
    assert state["attempts_used"] == 1


def test_api_count_can_flag_possible_truncation(
    tmp_path: Path, query: TradeQuery
) -> None:
    response_payload = payload([{"row": 1}], count=10)
    session = FakeSession([FakeResponse(response_payload)])
    client, _ = make_client(make_settings(tmp_path, api_key="secret"), session)

    with pytest.warns(RuntimeWarning, match="API_COUNT_EXCEEDS_RETURNED_RECORDS"):
        result = client.download_trade_data(replace(query, max_records=100))

    assert result.possibly_truncated
    assert result.warning_codes == ("API_COUNT_EXCEEDS_RETURNED_RECORDS",)


def test_refresh_keeps_timestamped_snapshots(
    tmp_path: Path, query: TradeQuery
) -> None:
    session = FakeSession(
        [FakeResponse(payload([{"version": 1}])), FakeResponse(payload([{"version": 2}]))]
    )
    fixed_now = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
    client, _ = make_client(
        make_settings(tmp_path, api_key="secret"), session, now=fixed_now
    )

    first = client.download_trade_data(query)
    refreshed = client.download_trade_data(query, refresh=True)

    assert first.payload_path != refreshed.payload_path
    assert first.payload_path.exists()
    assert refreshed.payload_path.exists()
    assert len(list(client.settings.raw_dir.glob("*.metadata.json"))) == 2


def test_corrupt_newest_snapshot_falls_back_to_older_valid_snapshot(
    tmp_path: Path, query: TradeQuery
) -> None:
    session = FakeSession(
        [FakeResponse(payload([{"version": 1}])), FakeResponse(payload([{"version": 2}]))]
    )
    fixed_now = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
    settings = make_settings(tmp_path, api_key="secret")
    client, _ = make_client(settings, session, now=fixed_now)
    older = client.download_trade_data(query)
    newer = client.download_trade_data(query, refresh=True)
    newer.payload_path.write_text("corrupt", encoding="utf-8")

    no_http_session = FakeSession()
    cache_client, _ = make_client(settings, no_http_session, now=fixed_now)
    cached = cache_client.download_trade_data(query)

    assert cached.from_cache
    assert cached.payload_path == older.payload_path
    assert cached.payload["data"] == [{"version": 1}]
    assert no_http_session.calls == []


@pytest.mark.parametrize("damage", ["missing-sidecar", "bad-checksum", "bad-count"])
def test_invalid_only_snapshot_is_not_reused(
    tmp_path: Path, query: TradeQuery, damage: str
) -> None:
    settings = make_settings(tmp_path, api_key="secret")
    first_session = FakeSession([FakeResponse(payload([{"version": 1}]))])
    first_client, _ = make_client(settings, first_session)
    first = first_client.download_trade_data(query)

    if damage == "missing-sidecar":
        first.metadata_path.unlink()
    else:
        metadata = json.loads(first.metadata_path.read_text(encoding="utf-8"))
        if damage == "bad-checksum":
            metadata["payload_sha256"] = "0" * 64
        else:
            metadata["record_count"] = 99
        first.metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    second_session = FakeSession([FakeResponse(payload([{"version": 2}]))])
    second_client, _ = make_client(settings, second_session)
    second = second_client.download_trade_data(query)

    assert second.from_cache is False
    assert second.payload["data"] == [{"version": 2}]
    assert len(second_session.calls) == 1


def test_snapshot_files_and_budget_never_contain_secret(
    tmp_path: Path, query: TradeQuery
) -> None:
    secret = "never-store-this-key"
    session = FakeSession([FakeResponse(payload([{"row": 1}]))])
    settings = make_settings(tmp_path, api_key=secret)
    client, _ = make_client(settings, session)

    result = client.download_trade_data(query)

    files = [result.payload_path, result.metadata_path, settings.budget_state_path]
    assert all(secret not in path.name for path in files)
    assert all(secret not in path.read_text(encoding="utf-8") for path in files)


def test_malformed_json_response_fails_cleanly(
    tmp_path: Path, query: TradeQuery
) -> None:
    session = FakeSession([FakeResponse(json_error=ValueError("bad json"))])
    client, _ = make_client(make_settings(tmp_path), session)

    with pytest.raises(ComtradeError, match="not valid JSON"):
        client.fetch_trade_data(query)


def test_compatibility_wrapper_builds_the_new_query(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    settings = make_settings(tmp_path, api_key="configured-key")
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        extract_module.ComtradeSettings,
        "from_env",
        classmethod(lambda cls: settings),
    )

    def fake_fetch(client: ComtradeClient, query: TradeQuery) -> dict[str, Any]:
        captured["settings"] = client.settings
        captured["query"] = query
        return payload([{"row": 1}])

    monkeypatch.setattr(extract_module.ComtradeClient, "fetch_trade_data", fake_fetch)

    result = extract_module.fetch_trade_data(
        type_code="C",
        frequency_code="M",
        classification_code="HS",
        period="202301",
        reporter_code="276",
        partner_code="0",
        commodity_code="TOTAL",
        flow_code="X",
        max_records=25,
    )

    assert result["count"] == 1
    assert captured["settings"] == settings
    assert captured["query"].max_records == 25
