# Global Trade Data Pipeline

A reproducible data-engineering portfolio project built around international
merchandise-trade data from the UN Comtrade API.

The project now includes a reliable extraction layer and an offline,
transactional DuckDB warehouse with version-controlled SQL transformations.

## Current capabilities

- authenticated UN Comtrade Data API access when `COMTRADE_API_KEY` is set;
- unauthenticated preview fallback when no key is configured;
- trade-data and metadata requests;
- multiple periods and flows in authenticated requests;
- deterministic retries and request pacing;
- a persisted local daily-attempt safeguard;
- validated, fingerprint-based raw JSON caching;
- timestamped refresh snapshots with non-sensitive provenance metadata;
- explicit warnings for responses that may have reached an API record limit;
- offline pytest coverage using mocked API responses.
- exact-manifest selection of immutable raw snapshots;
- typed staging, reusable core facts and dimensions, and analytical SQL models;
- atomic, idempotent DuckDB rebuilds with data-quality and reconciliation checks.

The exploratory notebooks are preserved under `notebooks/`.

## Setup

The project is verified with CPython 3.13.14.
DuckDB 1.5.6 is pinned because it is the stable build verified with that local
Python and Windows application-control environment.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
```

Copy `.env.example` to `.env` if needed and add your own key. The local
`.env` file is ignored by Git and must never be committed.

```dotenv
COMTRADE_API_KEY=
COMTRADE_RAW_DIR=data/raw
COMTRADE_REQUEST_INTERVAL_SECONDS=1.2
COMTRADE_TIMEOUT_SECONDS=30
COMTRADE_DAILY_REQUEST_BUDGET=100
COMTRADE_BUDGET_STATE_PATH=data/state/comtrade_request_budget.json
COMTRADE_WAREHOUSE_PATH=data/warehouse/trade.duckdb
COMTRADE_INGESTION_MANIFEST=config/iteration2_manifest.json
```

Process environment variables take precedence over values in `.env`.

## API modes and free-tier safeguards

When `COMTRADE_API_KEY` is present, the client uses the authenticated
`data/v1/get` and `data/v1/getMetadata` endpoints. Authenticated requests
support an explicit `max_records` up to 100,000. Without a key, the client
uses preview endpoints and limits requests to 500 records, one period, and one
commodity.

The default 1.2-second interval is deliberately more conservative than the
documented Free Basic limit of 1 API call per second. The service currently
documents a 500-call daily quota for this tier; see the
[UN Comtrade subscription limits](https://uncomtrade.org/docs/subscriptions/).
A local counter allows 100 outbound attempts per UTC day by default. Initial
calls, retries, preview calls, and metadata calls all consume this local
budget; validated cache hits do not.

The counter is stored in `data/state/comtrade_request_budget.json` and
survives process restarts. It only observes requests made through this project
client. It cannot count requests made by another application, machine,
notebook code using `requests` directly, or another process that bypasses the
client, and it does not replace UN Comtrade's server-enforced quota.

The client is designed for one active extraction process. This iteration does
not add distributed quota coordination.

## Small authenticated extraction

This example requests one aggregate record and never displays the API key.

```python
from src.config import ComtradeSettings
from src.extract import ComtradeClient, TradeQuery

settings = ComtradeSettings.from_env()
if not settings.api_key:
    raise RuntimeError("COMTRADE_API_KEY is not configured.")

client = ComtradeClient(settings)
query = TradeQuery(
    type_code="C",
    frequency_code="M",
    classification_code="HS",
    period="202301",
    reporter_code="276",
    partner_code="0",
    commodity_code="TOTAL",
    flow_code="X",
    max_records=10,
)

result = client.download_trade_data(query)

print("Records:", result.record_count)
print("Raw payload:", result.payload_path)
print("From cache:", result.from_cache)
print("Possibly truncated:", result.possibly_truncated)
```

Use `refresh=True` only when a new source snapshot is required:

```python
refreshed = client.download_trade_data(query, refresh=True)
```

## Raw snapshot contract

Each successful download produces:

- an unchanged JSON payload under `data/raw/`;
- a `.metadata.json` sidecar containing the credential-free request,
  fingerprint, retrieval time, payload checksum, row count, effective limit,
  and completeness warnings.

Before a cached payload is reused, both files, the checksum, fingerprint,
request, row count, and completeness fields are validated. Invalid snapshots
are left untouched for investigation and are not reused.

`possibly_truncated=True` means the returned row count reached the configured
limit or the API reported more rows than were returned. The raw response is
still preserved, but downstream transformations should reject it unless
incomplete input is explicitly acceptable. A false value means no truncation
signal was detected; it is not an independent proof that the upstream dataset
is complete.

This iteration does not implement pagination, automatic query splitting, bulk
downloads, or async extraction.

## Offline DuckDB warehouse

The warehouse build never calls UN Comtrade. The committed manifest pins exact
payload filenames, request fingerprints, checksums, request descriptors, and
record counts under the configured `data/raw` directory. A missing, changed,
truncated, or warned snapshot stops the build before the database is modified.

Run the complete offline rebuild with:

```powershell
.\.venv\Scripts\python.exe -m src.load
```

The loader validates all inputs first, then rebuilds every published table in
one DuckDB transaction. Repeating the command against the same manifest is
idempotent. Any SQL or quality-check failure rolls back the transaction and
leaves the previously committed warehouse intact.

The current manifest describes Germany's annual 2023–2024 imports and exports
for HS chapters 84, 85, and 87. Its 12-partition and 2,412-row expectations are
dataset-specific; the loader itself has no hardcoded reporter, period,
frequency, commodity, partition-count, or row-count assumptions.

### Data layers

- **Raw:** immutable JSON payloads and metadata sidecars under `data/raw/`.
- **Staging:** `staging.source_partitions` and typed
  `staging.trade_observation` rows with source lineage.
- **Core:** `core.fact_trade`, `core.dim_partner`, and `core.dim_product`.
- **Analytics:** annual HS2 chapter balances, non-World partner-area analysis,
  and extensible year-over-year comparisons.

The fact grain includes reporter, type, frequency, original period, flow,
classification version, commodity, partner, secondary partner, customs,
transport, and supply mode. Annual and monthly periods therefore cannot collide.
Monthly records are supported by staging and core schemas, but monthly marts are
not part of this iteration.

Monetary values use `DECIMAL(38,3)`. Request classification family (for example
`HS`) and response classification version (for example `H6`) are stored
separately. Analytics never merge classification revisions or commodity levels
implicitly.

`partnerCode=0` is classified as World. Every other value is deliberately
called a non-World partner area; the project does not assume that every code is
a sovereign country. World rows are excluded from partner rankings and used
explicitly for chapter totals.

### Manifest updates

Manifest snapshot references must be plain relative filenames. Absolute paths,
directory traversal, credential-shaped fields, authenticated query parameters,
and endpoints containing query strings are rejected. A refreshed snapshot is
not selected automatically: update its filename, fingerprint, checksum, request
descriptor, record count, and dataset expectations only after reviewing it.

The current reconciliation SQL compares the Notebook 04 chapter summaries with
a USD 5,000,001 tolerance, reflecting the half-unit tolerance of values rounded
to two decimal places in billions. It is selected by the dataset manifest and is
not a universal loader assumption.

## Tests

All automated tests are offline and use mocked responses.

```powershell
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m pytest
```

## Repository structure

```text
src/config.py
  Environment-backed settings

src/extract.py
  Query validation, API client, retries, safeguards, and raw snapshots

src/snapshots.py
  Shared immutable-snapshot integrity contract

src/load.py
  Offline manifest validation and transactional DuckDB build

config/iteration2_manifest.json
  Exact, credential-free dataset inputs

sql/
  Staging, core, analytics, quality, and dataset reconciliation models

tests/
  Offline unit tests

notebooks/01_public_api_exploration.ipynb
  Public preview exploration and source-grain inspection

notebooks/02_authenticated_api_exploration.ipynb
  Authenticated multi-period, multi-flow, and metadata exploration

data/raw/
  Ignored raw payloads and provenance sidecars

data/state/
  Ignored local request-budget state

data/warehouse/
  Ignored local DuckDB database and temporary files
```

## Planned later iterations

Future work may add monthly analytical models, broader classification and
partner reference data, orchestration, and presentation. Notebook 05 is not
created in this iteration.
