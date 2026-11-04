from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any

import duckdb
import pytest

from src.config import PROJECT_ROOT, WarehouseSettings
from src.load import LoadError, build_warehouse
from src.snapshots import request_fingerprint


def trade_record(
    *,
    period: str = "2023",
    frequency: str = "A",
    flow: str = "X",
    partner: str = "0",
    value: Any = 10.125,
    customs: str = "C00",
) -> dict[str, Any]:
    month = 52 if frequency == "A" else int(period[-2:])
    return {
        "typeCode": "C",
        "freqCode": frequency,
        "period": period,
        "refYear": int(period[:4]),
        "refMonth": month,
        "reporterCode": 276,
        "reporterISO": "DEU",
        "reporterDesc": "Germany",
        "flowCode": flow,
        "flowDesc": "Exports" if flow == "X" else "Imports",
        "partnerCode": int(partner),
        "partnerISO": "W00" if partner == "0" else "TST",
        "partnerDesc": "World" if partner == "0" else "Test partner",
        "partner2Code": 0,
        "partner2ISO": "W00",
        "partner2Desc": "World",
        "classificationSearchCode": "HS",
        "classificationCode": "H6",
        "isOriginalClassification": True,
        "cmdCode": "84",
        "cmdDesc": "Machinery",
        "aggrLevel": 2,
        "isLeaf": False,
        "isAggregate": True,
        "customsCode": customs,
        "customsDesc": "Total customs",
        "motCode": 0,
        "motDesc": "All modes",
        "mosCode": "0",
        "primaryValue": value,
        "cifvalue": value if flow == "M" else None,
        "fobvalue": value if flow == "X" else None,
        "isReported": False,
        "legacyEstimationFlag": 0,
    }


def request_for(record: dict[str, Any], max_records: int = 100) -> dict[str, Any]:
    return {
        "request_kind": "trade",
        "mode": "authenticated",
        "endpoint": (
            "https://comtradeapi.un.org/data/v1/get/C/"
            f"{record['freqCode']}/HS"
        ),
        "query_parameters": {
            "period": str(record["period"]),
            "reporterCode": "276",
            "cmdCode": "84",
            "flowCode": record["flowCode"],
            "maxRecords": max_records,
            "breakdownMode": "classic",
            "includeDesc": "true",
        },
    }


def write_partition(
    raw_dir: Path,
    partition_id: str,
    records: list[dict[str, Any]],
    *,
    request: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    selected_request = request or request_for(records[0])
    fingerprint = request_fingerprint(selected_request)
    filename = f"snapshot_{partition_id}.json"
    payload_path = raw_dir / filename
    payload = {"count": len(records), "data": records, "error": ""}
    payload_bytes = (
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8") + b"\n"
    )
    payload_path.write_bytes(payload_bytes)
    checksum = hashlib.sha256(payload_bytes).hexdigest()
    metadata = {
        "schema_version": 1,
        "request_fingerprint": fingerprint,
        "request": selected_request,
        "retrieved_at_utc": "2026-11-04T12:00:00Z",
        "payload_sha256": checksum,
        "record_count": len(records),
        "api_count": len(records),
        "max_records": selected_request["query_parameters"]["maxRecords"],
        "possibly_truncated": False,
        "completeness_status": "no_truncation_signal",
        "warning_codes": [],
    }
    payload_path.with_name(f"{payload_path.stem}.metadata.json").write_text(
        json.dumps(metadata), encoding="utf-8"
    )
    return {
        "partition_id": partition_id,
        "payload_file": filename,
        "request_fingerprint": fingerprint,
        "payload_sha256": checksum,
        "record_count": len(records),
        "request": selected_request,
    }


def make_project(
    tmp_path: Path, partitions: list[dict[str, Any]]
) -> tuple[WarehouseSettings, dict[str, Any]]:
    sql_dir = tmp_path / "sql"
    shutil.copytree(PROJECT_ROOT / "sql", sql_dir)
    manifest = {
        "schema_version": 1,
        "dataset_id": "synthetic",
        "expected_partition_count": len(partitions),
        "expected_record_count": sum(item["record_count"] for item in partitions),
        "partitions": partitions,
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    settings = WarehouseSettings(
        raw_dir=tmp_path / "raw",
        warehouse_path=tmp_path / "warehouse" / "test.duckdb",
        manifest_path=manifest_path,
        sql_dir=sql_dir,
    )
    return settings, manifest


def test_build_is_idempotent_and_preserves_decimal_lineage(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    partitions = [
        write_partition(raw, "2023_M", [trade_record(flow="M", value=4.125)]),
        write_partition(raw, "2023_X", [trade_record(flow="X", value=10.125)]),
    ]
    settings, _ = make_project(tmp_path, partitions)

    first = build_warehouse(settings)
    second = build_warehouse(settings)

    assert first.fact_record_count == second.fact_record_count == 2
    connection = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        rows = connection.execute(
            "SELECT flow_code, trade_value_usd, source_file "
            "FROM core.fact_trade ORDER BY flow_code"
        ).fetchall()
        balance = connection.execute(
            "SELECT trade_balance_usd FROM analytics.chapter_trade_balance"
        ).fetchone()[0]
    finally:
        connection.close()
    assert str(rows[0][1]) == "4.125"
    assert str(rows[1][1]) == "10.125"
    assert all(row[2].startswith("snapshot_") for row in rows)
    assert str(balance) == "6.000"


def test_monthly_rows_are_supported_but_excluded_from_annual_marts(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw"
    annual = write_partition(raw, "annual", [trade_record(value=10)])
    monthly_record = trade_record(period="202403", frequency="M", value=7)
    monthly = write_partition(raw, "monthly", [monthly_record])
    settings, _ = make_project(tmp_path, [annual, monthly])

    build_warehouse(settings)

    connection = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        periods = connection.execute(
            "SELECT frequency_code, period_year, period_month, period_start_date "
            "FROM staging.trade_observation ORDER BY frequency_code"
        ).fetchall()
        annual_mart_rows = connection.execute(
            "SELECT count(*) FROM analytics.chapter_yoy"
        ).fetchone()[0]
    finally:
        connection.close()
    assert periods[0][0:3] == ("A", 2023, None)
    assert periods[1][0:3] == ("M", 2024, 3)
    assert str(periods[1][3]) == "2024-03-01"
    assert annual_mart_rows == 1


def test_expanded_grain_keeps_customs_breakdowns_distinct(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    records = [trade_record(customs="C00"), trade_record(customs="C01")]
    partition = write_partition(raw, "customs", records)
    settings, _ = make_project(tmp_path, [partition])

    result = build_warehouse(settings)

    assert result.fact_record_count == 2


def test_duplicate_complete_fact_key_fails(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    record = trade_record()
    partition = write_partition(raw, "duplicate", [record, deepcopy(record)])
    settings, _ = make_project(tmp_path, [partition])

    with pytest.raises(LoadError, match="fact_grain_unique"):
        build_warehouse(settings)


@pytest.mark.parametrize(
    "unsafe_name",
    ["../snapshot.json", "nested/snapshot.json", "C:\\snapshot.json", ""],
)
def test_manifest_rejects_unsafe_snapshot_paths(
    tmp_path: Path, unsafe_name: str
) -> None:
    raw = tmp_path / "raw"
    partition = write_partition(raw, "safe", [trade_record()])
    settings, manifest = make_project(tmp_path, [partition])
    manifest["partitions"][0]["payload_file"] = unsafe_name
    settings.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(LoadError, match="filename"):
        build_warehouse(settings)


def test_manifest_rejects_credentials(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    partition = write_partition(raw, "safe", [trade_record()])
    settings, manifest = make_project(tmp_path, [partition])
    manifest["api_key"] = "must-not-be-stored"
    settings.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(LoadError, match="Credential-shaped"):
        build_warehouse(settings)


def test_exact_manifest_snapshot_is_used_when_history_exists(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    pinned = write_partition(raw, "pinned", [trade_record(value=10)])
    write_partition(raw, "newer_unlisted", [trade_record(value=999)])
    settings, _ = make_project(tmp_path, [pinned])

    build_warehouse(settings)

    connection = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        value = connection.execute(
            "SELECT trade_value_usd FROM core.fact_trade"
        ).fetchone()[0]
    finally:
        connection.close()
    assert str(value) == "10.000"


def test_tampered_or_warned_snapshot_fails_closed(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    partition = write_partition(raw, "tampered", [trade_record()])
    settings, _ = make_project(tmp_path, [partition])
    payload_path = raw / partition["payload_file"]
    payload_path.write_text("{}", encoding="utf-8")

    with pytest.raises(LoadError, match="invalid"):
        build_warehouse(settings)


def test_invalid_decimal_precision_is_rejected(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    partition = write_partition(
        raw, "precision", [trade_record(value=10.1234)]
    )
    settings, _ = make_project(tmp_path, [partition])

    with pytest.raises(LoadError, match="three fractional digits"):
        build_warehouse(settings)


def test_failed_transaction_preserves_previous_warehouse(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    partition = write_partition(raw, "stable", [trade_record(value=10)])
    settings, _ = make_project(tmp_path, [partition])
    build_warehouse(settings)

    (settings.sql_dir / "04_quality_checks.sql").write_text(
        "SELECT 'forced_failure', 1::BIGINT, 'rollback test'",
        encoding="utf-8",
    )
    with pytest.raises(LoadError, match="forced_failure"):
        build_warehouse(settings)

    connection = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        count, value = connection.execute(
            "SELECT count(*), max(trade_value_usd) FROM core.fact_trade"
        ).fetchone()
    finally:
        connection.close()
    assert count == 1
    assert str(value) == "10.000"


def test_yoy_handles_zero_prior_value(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    prior = write_partition(raw, "2023", [trade_record(period="2023", value=0)])
    current = write_partition(raw, "2024", [trade_record(period="2024", value=5)])
    settings, _ = make_project(tmp_path, [prior, current])
    build_warehouse(settings)

    connection = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        row = connection.execute(
            "SELECT prior_year, absolute_change_usd, percentage_change "
            "FROM analytics.chapter_yoy WHERE current_year = 2024"
        ).fetchone()
    finally:
        connection.close()
    assert row[0] == 2023
    assert str(row[1]) == "5.000"
    assert row[2] is None
