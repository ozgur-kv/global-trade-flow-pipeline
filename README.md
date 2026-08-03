# Global Trade & Supply-Chain Explorer

A reproducible data pipeline that retrieves international trade data,
validates and models it in DuckDB, calculates trade and supply-chain
metrics with SQL, and presents trends through a Streamlit dashboard.

## Business Question

How have Germany's and Turkey's imports and exports changed over time,
and which products, trading partners etc. drive those movements?

## Data Source

The project uses international merchandise-trade data from the
UN Comtrade API.

Data can be retrieved by dimensions such as:
- reporting country
- partner country
- commodity
- trade flow
- period

## Planned Architecture

```text
UN Comtrade API
→ raw JSON
→ Python normalization and validation
→ DuckDB staging and analytical model
→ SQL marts
→ Streamlit dashboard
```

## Current Status

Completed:

- repository and environment setup
- first public UN Comtrade request
- multiple requests handling
- JSON response and record inspection
- timestamped raw JSON storage
- reusable public-preview extraction functions

The current extractor uses the public preview endpoint and is intended
for development and structural exploration.

## Repository Structure

```text
notebooks/01_api_exploration.ipynb
→ API exploration, source interpretation and grain analysis

src/extract.py
→ reusable request validation and raw-payload saving

data/raw/
→ local source responses, excluded from Git
