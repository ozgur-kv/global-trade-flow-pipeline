# Input: UN Comtrade request parameters
# Process: Build endpoint --> Send GET request --> Validate HTTP response --> Parse and validate payload
# Output: Complete payload dictionary
# We need the intended behavior to be parametric:
# no subscription key
# → public preview API
# subscription key provided
# → authenticated final-data API

from typing import Any
import requests
import json
from datetime import datetime
from pathlib import Path

# BASE_URL = "https://comtradeapi.un.org/public/v1/preview"

PREVIEW_BASE_URL = "https://comtradeapi.un.org/public/v1/preview"
FINAL_DATA_BASE_URL = "https://comtradeapi.un.org/data/v1/get"

def fetch_trade_data(
        *, # every parameter after * must be passed with name
        type_code: str,
        frequency_code: str,
        classification_code: str,
        period: str,
        reporter_code: str,
        partner_code: str,
        commodity_code: str,
        flow_code: str,
        subscription_key: str | None = None, # either a string or None
        max_records: int = 500, # we can adjust this to get more data per call
        timeout: int = 30,
) -> dict[str, Any]: # keys are strings, values can be different types
    # select base url based on subscription key availability
    base_url = FINAL_DATA_BASE_URL if subscription_key else PREVIEW_BASE_URL
    # create the url with required path parameters
    url = (
        f"{base_url}/"
        f"{type_code}/"
        f"{frequency_code}/"
        f"{classification_code}"
    )
    # select query parameters
    params = {
        'period': period,
        'reporterCode': reporter_code,
        'partnerCode': partner_code,
        'cmdCode': commodity_code,
        'flowCode': flow_code,
        'maxRecords': max_records,
        'breakdownMode': 'classic', # or 'plus' for extended breakdown dimensions
        'includeDesc': 'true',
    }
    # add subscription key to query parameters if it exists
    if subscription_key:
        params["subscription-key"] = subscription_key
    # get the response
    response = requests.get(
        url,
        params=params,
        timeout=timeout,
    )
    # check for successful call
    response.raise_for_status()
    # parse response into dictionary
    payload = response.json()
    # valdiation checks
    # check if responsehas been converted to dictionary format
    if not isinstance(payload, dict):
        raise TypeError('API response is expected to be a dictionary.')
    # get request specific errors, if exist
    api_error = payload.get('error')
    if api_error:
        raise ValueError(f'UN ComTrade has returned an error: {api_error}')
    # check if data is in list format
    records = payload.get('data')
    if not isinstance(records, list):
        raise TypeError("Expected payload['data'] to be a list")

    return payload

def save_raw_payload(
    payload: dict[str, Any],
    *,
    output_directory: Path,
    reporter_code: str,
    partner_code: str,
    flow_code: str,
    period: str,
) -> Path:
    output_directory.mkdir( # create folder if it doesn't exist
        parents=True, # parent folders are also created
        exist_ok=True, # if file already exists, do not give error
    )
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") # get ts for naming the file
    filename = (
        f"comtrade_"
        f"reporter-{reporter_code}_"
        f"partner-{partner_code}_"
        f"flow-{flow_code}_"
        f"period-{period}_"
        f"{timestamp}.json"
    )
    output_path = output_directory / filename
    with output_path.open("w", encoding="utf-8") as file: # 'w': write mode: if file doesn't exist - create, else replace | utf-8 standard text encoding for JSON
        json.dump( # write to file
            payload,
            file,
            ensure_ascii=False, # keep words readable e.g. ö,ü
            indent=2, # use 2 spaces per indentation level
        )
    print('Saved raw response to:')
    print(output_path)

    return output_path