# Global Trade Data Pipeline

A reproducible data-engineering portfolio project built around international
merchandise-trade data from the UN Comtrade API.

This first iteration focuses on a reliable extraction layer. Transformation,
DuckDB modeling, orchestration, and presentation are intentionally deferred.

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

The exploratory notebooks are preserved under `notebooks/`.

## Setup

The project is verified with CPython 3.13.14.

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
```

## Planned later iterations

Future work may add normalization, data-quality checks, DuckDB models, SQL
marts, orchestration, and presentation. Those components are outside the scope
of the current extraction iteration.
