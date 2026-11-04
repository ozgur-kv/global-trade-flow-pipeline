"""Pure validation helpers for immutable UN Comtrade snapshots."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any


SNAPSHOT_METADATA_VERSION = 1


class SnapshotValidationError(ValueError):
    """Raised when a raw snapshot does not satisfy its integrity contract."""


@dataclass(frozen=True, slots=True)
class ValidatedSnapshot:
    payload: dict[str, Any]
    payload_bytes: bytes
    metadata: dict[str, Any]
    records: list[Any]
    possibly_truncated: bool
    warning_codes: tuple[str, ...]


def request_fingerprint(descriptor: dict[str, Any]) -> str:
    canonical = json.dumps(
        descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def completeness(
    payload: dict[str, Any], records: list[Any], max_records: int
) -> tuple[bool, tuple[str, ...]]:
    warning_codes: list[str] = []
    if len(records) >= max_records:
        warning_codes.append("MAX_RECORD_LIMIT_REACHED")
    api_count = payload.get("count")
    if isinstance(api_count, int) and api_count > len(records):
        warning_codes.append("API_COUNT_EXCEEDS_RETURNED_RECORDS")
    return bool(warning_codes), tuple(warning_codes)


def validate_snapshot_pair(
    payload_path: Path,
    metadata_path: Path,
    *,
    expected_request: dict[str, Any],
    expected_fingerprint: str,
    expected_max_records: int,
    expected_checksum: str | None = None,
    expected_record_count: int | None = None,
) -> ValidatedSnapshot:
    """Validate one payload/sidecar pair without modifying either file."""

    try:
        payload_bytes = payload_path.read_bytes()
        payload = json.loads(payload_bytes.decode("utf-8"))
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotValidationError("Snapshot files are unreadable or invalid JSON.") from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise SnapshotValidationError("Snapshot payload must contain a data list.")
    if payload.get("error"):
        raise SnapshotValidationError("Snapshot payload contains an API error.")
    if (
        not isinstance(payload.get("count"), int)
        or isinstance(payload.get("count"), bool)
        or payload["count"] < 0
    ):
        raise SnapshotValidationError(
            "Snapshot payload count must be a non-negative integer."
        )
    if not isinstance(metadata, dict):
        raise SnapshotValidationError("Snapshot metadata must be a JSON object.")

    records = payload["data"]
    possibly_truncated, warning_codes = completeness(
        payload, records, expected_max_records
    )
    completeness_status = (
        "possibly_truncated" if possibly_truncated else "no_truncation_signal"
    )
    checksum = hashlib.sha256(payload_bytes).hexdigest()

    checks = {
        "metadata schema": metadata.get("schema_version") == SNAPSHOT_METADATA_VERSION,
        "request fingerprint": metadata.get("request_fingerprint")
        == expected_fingerprint,
        "request descriptor": metadata.get("request") == expected_request,
        "retrieval timestamp": isinstance(metadata.get("retrieved_at_utc"), str),
        "payload checksum": metadata.get("payload_sha256") == checksum,
        "record count": metadata.get("record_count") == len(records),
        "record limit": metadata.get("max_records") == expected_max_records,
        "API count": metadata.get("api_count") == payload.get("count"),
        "truncation flag": metadata.get("possibly_truncated") is possibly_truncated,
        "completeness status": metadata.get("completeness_status")
        == completeness_status,
        "warning codes": metadata.get("warning_codes") == list(warning_codes),
    }
    if expected_checksum is not None:
        checks["manifest checksum"] = checksum == expected_checksum
    if expected_record_count is not None:
        checks["manifest record count"] = len(records) == expected_record_count

    failed = [name for name, valid in checks.items() if not valid]
    if failed:
        raise SnapshotValidationError(
            "Snapshot validation failed: " + ", ".join(failed) + "."
        )

    return ValidatedSnapshot(
        payload=payload,
        payload_bytes=payload_bytes,
        metadata=metadata,
        records=records,
        possibly_truncated=possibly_truncated,
        warning_codes=warning_codes,
    )
