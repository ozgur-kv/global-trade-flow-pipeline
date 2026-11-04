"""Offline, deterministic loading of validated snapshots into DuckDB."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path, PurePath
import re
from typing import Any
from urllib.parse import urlsplit

import duckdb

from src.config import WarehouseSettings
from src.snapshots import (
    SnapshotValidationError,
    request_fingerprint,
    validate_snapshot_pair,
)


MANIFEST_SCHEMA_VERSION = 1
_CREDENTIAL_KEY = re.compile(
    r"(?i)(subscription|api[-_ ]?key|authorization|token|secret|password|credential)"
)
_MONEY = re.compile(r"^[0-9]{1,35}(?:\.[0-9]{1,3})?$")


class LoadError(RuntimeError):
    """Raised when an offline warehouse build cannot be published safely."""


@dataclass(frozen=True, slots=True)
class IngestionSummary:
    dataset_id: str
    warehouse_path: Path
    partition_count: int
    source_record_count: int
    staging_record_count: int
    fact_record_count: int
    partner_count: int
    product_count: int
    chapter_balance_count: int
    partner_analysis_count: int
    chapter_yoy_count: int
    reconciliation_checks: int


def _safe_relative_file(base: Path, filename: Any, label: str) -> Path:
    if not isinstance(filename, str) or not filename:
        raise LoadError(f"{label} must be a non-empty relative filename.")
    candidate_name = PurePath(filename)
    if (
        candidate_name.is_absolute()
        or candidate_name.drive
        or len(candidate_name.parts) != 1
        or candidate_name.name in {".", ".."}
        or candidate_name.name != filename
    ):
        raise LoadError(f"{label} must be a filename without directory components.")

    base_resolved = base.resolve()
    candidate = base / filename
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise LoadError(f"Required {label} is unavailable: {filename}") from exc
    if not resolved.is_relative_to(base_resolved):
        raise LoadError(f"{label} escapes the configured directory: {filename}")
    if not resolved.is_file():
        raise LoadError(f"Required {label} is not a file: {filename}")
    return resolved


def _safe_sql_path(sql_dir: Path, relative_path: Any) -> Path:
    if not isinstance(relative_path, str) or not relative_path:
        raise LoadError("reconciliation_sql must be a non-empty relative path.")
    pure = PurePath(relative_path)
    if pure.is_absolute() or pure.drive or ".." in pure.parts:
        raise LoadError("reconciliation_sql must remain under the SQL directory.")
    base = sql_dir.resolve()
    try:
        resolved = (sql_dir / relative_path).resolve(strict=True)
    except OSError as exc:
        raise LoadError("Configured reconciliation SQL is unavailable.") from exc
    if not resolved.is_relative_to(base) or not resolved.is_file():
        raise LoadError("reconciliation_sql must remain under the SQL directory.")
    return resolved


def _reject_credentials(value: Any, path: str = "manifest") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if _CREDENTIAL_KEY.search(str(key)):
                raise LoadError(f"Credential-shaped field is not allowed at {path}.")
            _reject_credentials(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_credentials(item, f"{path}[{index}]")


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LoadError("Ingestion manifest is unreadable or invalid JSON.") from exc
    if not isinstance(manifest, dict):
        raise LoadError("Ingestion manifest must be a JSON object.")
    _reject_credentials(manifest)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise LoadError("Unsupported ingestion manifest schema version.")
    if not isinstance(manifest.get("dataset_id"), str) or not manifest["dataset_id"]:
        raise LoadError("Manifest dataset_id must be a non-empty string.")
    partitions = manifest.get("partitions")
    if not isinstance(partitions, list) or not partitions:
        raise LoadError("Manifest must contain at least one partition.")
    for field in ("expected_partition_count", "expected_record_count"):
        value = manifest.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise LoadError(f"Manifest {field} must be a non-negative integer.")
    return manifest


def _validate_request(request: Any, fingerprint: Any) -> tuple[dict[str, Any], int]:
    if not isinstance(request, dict):
        raise LoadError("Each partition request must be a JSON object.")
    endpoint = request.get("endpoint")
    params = request.get("query_parameters")
    if not isinstance(endpoint, str) or not isinstance(params, dict):
        raise LoadError("Partition request endpoint and query parameters are required.")
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "comtradeapi.un.org"
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise LoadError("Partition endpoint must be a credential-free UN Comtrade URL.")
    _reject_credentials(params, "request.query_parameters")
    max_records = params.get("maxRecords")
    if not isinstance(max_records, int) or isinstance(max_records, bool) or max_records <= 0:
        raise LoadError("Partition request maxRecords must be a positive integer.")
    if not isinstance(fingerprint, str) or request_fingerprint(request) != fingerprint:
        raise LoadError("Partition request fingerprint does not match its descriptor.")
    return request, max_records


def _validate_money(record: dict[str, Any], field: str, required: bool) -> None:
    value = record.get(field)
    if value is None and not required:
        return
    text = str(value)
    if value is None or _MONEY.fullmatch(text) is None:
        raise LoadError(
            f"Source field {field} must be a non-negative decimal with at most "
            "three fractional digits."
        )


def _validate_record_scope(record: Any, request: dict[str, Any]) -> None:
    if not isinstance(record, dict):
        raise LoadError("Every source data item must be a JSON object.")
    required = {
        "typeCode",
        "freqCode",
        "period",
        "reporterCode",
        "flowCode",
        "classificationCode",
        "classificationSearchCode",
        "cmdCode",
        "partnerCode",
        "partner2Code",
        "customsCode",
        "motCode",
        "mosCode",
        "primaryValue",
    }
    missing = required - record.keys()
    if missing:
        raise LoadError(f"Source record is missing required fields: {sorted(missing)}")

    parts = request["endpoint"].rstrip("/").split("/")
    if len(parts) < 3:
        raise LoadError("Partition endpoint does not contain type/frequency/classification.")
    params = request["query_parameters"]
    expected_sets = {
        "typeCode": {parts[-3]},
        "freqCode": {parts[-2]},
        "classificationSearchCode": {parts[-1]},
        "period": set(str(params["period"]).split(",")),
        "reporterCode": set(str(params["reporterCode"]).split(",")),
        "flowCode": set(str(params["flowCode"]).split(",")),
        "cmdCode": set(str(params["cmdCode"]).split(",")),
    }
    optional = {
        "partnerCode": "partnerCode",
        "partner2Code": "partner2Code",
        "customsCode": "customsCode",
        "motCode": "motCode",
    }
    for field, param in optional.items():
        if param in params:
            expected_sets[field] = set(str(params[param]).split(","))
    for field, allowed in expected_sets.items():
        if str(record.get(field)) not in allowed:
            raise LoadError(f"Source record field {field} is outside request scope.")

    _validate_money(record, "primaryValue", required=True)
    _validate_money(record, "cifvalue", required=False)
    _validate_money(record, "fobvalue", required=False)


def _prepare_inputs(
    settings: WarehouseSettings, manifest: dict[str, Any]
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], Path | None]:
    partition_rows: list[tuple[Any, ...]] = []
    record_rows: list[tuple[Any, ...]] = []
    seen_ids: set[str] = set()
    seen_files: set[str] = set()
    seen_fingerprints: set[str] = set()

    for partition in manifest["partitions"]:
        if not isinstance(partition, dict):
            raise LoadError("Every manifest partition must be a JSON object.")
        partition_id = partition.get("partition_id")
        filename = partition.get("payload_file")
        fingerprint = partition.get("request_fingerprint")
        checksum = partition.get("payload_sha256")
        record_count = partition.get("record_count")
        if not isinstance(partition_id, str) or not partition_id:
            raise LoadError("Every partition requires a non-empty partition_id.")
        if (
            not isinstance(filename, str)
            or not filename.endswith(".json")
            or filename.endswith(".metadata.json")
        ):
            raise LoadError("Every partition requires a JSON payload filename.")
        if not isinstance(fingerprint, str):
            raise LoadError("Every partition requires a request fingerprint.")
        if partition_id in seen_ids or filename in seen_files or fingerprint in seen_fingerprints:
            raise LoadError("Partition IDs, payload files, and fingerprints must be unique.")
        seen_ids.add(partition_id)
        seen_files.add(filename)
        seen_fingerprints.add(fingerprint)
        if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise LoadError("Partition payload_sha256 must be a lowercase SHA-256 value.")
        if not isinstance(record_count, int) or isinstance(record_count, bool) or record_count < 0:
            raise LoadError("Partition record_count must be a non-negative integer.")

        request, max_records = _validate_request(partition.get("request"), fingerprint)
        payload_path = _safe_relative_file(settings.raw_dir, filename, "payload file")
        metadata_name = f"{payload_path.stem}.metadata.json"
        metadata_path = _safe_relative_file(
            settings.raw_dir, metadata_name, "metadata file"
        )
        try:
            snapshot = validate_snapshot_pair(
                payload_path,
                metadata_path,
                expected_request=request,
                expected_fingerprint=fingerprint,
                expected_max_records=max_records,
                expected_checksum=checksum,
                expected_record_count=record_count,
            )
        except SnapshotValidationError as exc:
            raise LoadError(f"Partition {partition_id} is invalid: {exc}") from exc
        if snapshot.possibly_truncated or snapshot.warning_codes:
            raise LoadError(f"Partition {partition_id} has unresolved completeness warnings.")

        exact_payload = json.loads(
            snapshot.payload_bytes.decode("utf-8"), parse_int=str, parse_float=str
        )
        for ordinal, record in enumerate(exact_payload["data"]):
            _validate_record_scope(record, request)
            record_rows.append(
                (
                    partition_id,
                    ordinal,
                    json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                )
            )
        metadata = snapshot.metadata
        partition_rows.append(
            (
                manifest["dataset_id"],
                partition_id,
                filename,
                metadata_name,
                fingerprint,
                checksum,
                metadata["retrieved_at_utc"],
                record_count,
                max_records,
                metadata["api_count"],
                json.dumps(request, ensure_ascii=False, sort_keys=True),
            )
        )

    if len(partition_rows) != manifest["expected_partition_count"]:
        raise LoadError("Manifest partition count does not match its expectation.")
    if len(record_rows) != manifest["expected_record_count"]:
        raise LoadError("Validated source record count does not match its expectation.")

    reconciliation = None
    if manifest.get("reconciliation_sql") is not None:
        reconciliation = _safe_sql_path(
            settings.sql_dir, manifest["reconciliation_sql"]
        )
    return partition_rows, record_rows, reconciliation


def _read_sql(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        raise LoadError(f"Required SQL model is unavailable: {path.name}") from exc


def _run_checks(connection: duckdb.DuckDBPyConnection, sql: str, label: str) -> int:
    rows = connection.execute(sql).fetchall()
    failures = [(name, count, detail) for name, count, detail in rows if count]
    if failures:
        summary = "; ".join(f"{name}={count} ({detail})" for name, count, detail in failures)
        raise LoadError(f"{label} failed: {summary}")
    return len(rows)


def build_warehouse(settings: WarehouseSettings) -> IngestionSummary:
    """Validate pinned snapshots and atomically rebuild the local warehouse."""

    manifest = _load_manifest(settings.manifest_path)
    partition_rows, record_rows, reconciliation = _prepare_inputs(settings, manifest)
    model_paths = [settings.sql_dir / name for name in (
        "01_staging.sql", "02_core.sql", "03_analytics.sql", "04_quality_checks.sql"
    )]
    model_sql = [_read_sql(path) for path in model_paths]

    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.warehouse_path))
    reconciliation_checks = 0
    try:
        connection.execute("BEGIN TRANSACTION")
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE _source_partitions (
                dataset_id VARCHAR, partition_id VARCHAR, source_file VARCHAR,
                metadata_file VARCHAR, request_fingerprint VARCHAR,
                payload_sha256 VARCHAR, retrieved_at_utc VARCHAR,
                record_count INTEGER, max_records INTEGER, api_count INTEGER,
                request_json JSON
            )
            """
        )
        connection.executemany(
            "INSERT INTO _source_partitions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            partition_rows,
        )
        connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE _source_records (
                partition_id VARCHAR, source_record_ordinal INTEGER, record_json JSON
            )
            """
        )
        connection.executemany(
            "INSERT INTO _source_records VALUES (?, ?, ?)", record_rows
        )
        for sql in model_sql[:3]:
            connection.execute(sql)
        _run_checks(connection, model_sql[3], "Warehouse quality checks")
        if reconciliation is not None:
            reconciliation_checks = _run_checks(
                connection,
                _read_sql(reconciliation),
                "Dataset reconciliation",
            )
        connection.execute("COMMIT")
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise
    finally:
        connection.close()

    connection = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        counts = {
            name: connection.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            for name in (
                "staging.trade_observation",
                "core.fact_trade",
                "core.dim_partner",
                "core.dim_product",
                "analytics.chapter_trade_balance",
                "analytics.partner_trade_analysis",
                "analytics.chapter_yoy",
            )
        }
    finally:
        connection.close()
    return IngestionSummary(
        dataset_id=manifest["dataset_id"],
        warehouse_path=settings.warehouse_path,
        partition_count=len(partition_rows),
        source_record_count=len(record_rows),
        staging_record_count=counts["staging.trade_observation"],
        fact_record_count=counts["core.fact_trade"],
        partner_count=counts["core.dim_partner"],
        product_count=counts["core.dim_product"],
        chapter_balance_count=counts["analytics.chapter_trade_balance"],
        partner_analysis_count=counts["analytics.partner_trade_analysis"],
        chapter_yoy_count=counts["analytics.chapter_yoy"],
        reconciliation_checks=reconciliation_checks,
    )


def _parse_args() -> argparse.Namespace:
    defaults = WarehouseSettings.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=defaults.raw_dir)
    parser.add_argument("--manifest", type=Path, default=defaults.manifest_path)
    parser.add_argument("--database", type=Path, default=defaults.warehouse_path)
    parser.add_argument("--sql-dir", type=Path, default=defaults.sql_dir)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    summary = build_warehouse(
        WarehouseSettings(
            raw_dir=args.raw_dir,
            warehouse_path=args.database,
            manifest_path=args.manifest,
            sql_dir=args.sql_dir,
        )
    )
    print(json.dumps({
        "dataset_id": summary.dataset_id,
        "warehouse_path": str(summary.warehouse_path),
        "partition_count": summary.partition_count,
        "source_record_count": summary.source_record_count,
        "staging_record_count": summary.staging_record_count,
        "fact_record_count": summary.fact_record_count,
        "partner_count": summary.partner_count,
        "product_count": summary.product_count,
        "chapter_balance_count": summary.chapter_balance_count,
        "partner_analysis_count": summary.partner_analysis_count,
        "chapter_yoy_count": summary.chapter_yoy_count,
        "reconciliation_checks": summary.reconciliation_checks,
    }, indent=2))


if __name__ == "__main__":
    main()
