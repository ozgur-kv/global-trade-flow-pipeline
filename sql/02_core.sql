CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE TABLE core.dim_partner AS
SELECT DISTINCT
    partner_code,
    partner_iso,
    partner_description,
    partner_code = '0' AS is_world,
    CASE WHEN partner_code = '0' THEN 'world' ELSE 'non_world_partner_area' END
        AS partner_scope
FROM staging.trade_observation;

CREATE OR REPLACE TABLE core.dim_product AS
SELECT DISTINCT
    classification_version_code,
    commodity_code,
    classification_family_code,
    commodity_description,
    commodity_level,
    is_leaf,
    is_aggregate,
    is_total_commodity
FROM staging.trade_observation;

CREATE OR REPLACE TABLE core.fact_trade AS
SELECT
    reporter_code,
    type_code,
    frequency_code,
    period_code,
    period_year,
    period_month,
    period_start_date,
    flow_code,
    classification_family_code,
    classification_version_code,
    commodity_code,
    partner_code,
    partner2_code,
    customs_code,
    mode_of_transport_code,
    mode_of_supply_code,
    trade_value_usd,
    cif_value_usd,
    fob_value_usd,
    is_original_classification,
    is_reported,
    legacy_estimation_flag,
    partition_id,
    source_file,
    source_record_ordinal,
    request_fingerprint,
    payload_sha256,
    retrieved_at_utc
FROM staging.trade_observation;
