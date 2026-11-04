CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE TABLE staging.source_partitions AS
SELECT
    dataset_id,
    partition_id,
    source_file,
    metadata_file,
    request_fingerprint,
    payload_sha256,
    CAST(retrieved_at_utc AS TIMESTAMP) AS retrieved_at_utc,
    record_count,
    max_records,
    api_count,
    request_json
FROM _source_partitions;

CREATE OR REPLACE TABLE staging.trade_observation AS
WITH source AS (
    SELECT
        r.partition_id,
        r.source_record_ordinal,
        r.record_json,
        p.source_file,
        p.request_fingerprint,
        p.payload_sha256,
        CAST(p.retrieved_at_utc AS TIMESTAMP) AS retrieved_at_utc
    FROM _source_records r
    JOIN _source_partitions p USING (partition_id)
), normalized AS (
    SELECT
        *,
        json_extract_string(record_json, '$.freqCode') AS frequency_code,
        json_extract_string(record_json, '$.period') AS period_code
    FROM source
)
SELECT
    partition_id,
    json_extract_string(record_json, '$.typeCode') AS type_code,
    frequency_code,
    period_code,
    CASE
        WHEN frequency_code = 'A' AND regexp_full_match(period_code, '[0-9]{4}')
            THEN TRY_CAST(period_code AS INTEGER)
        WHEN frequency_code = 'M' AND regexp_full_match(period_code, '[0-9]{6}')
            THEN TRY_CAST(substr(period_code, 1, 4) AS INTEGER)
    END AS period_year,
    CASE
        WHEN frequency_code = 'M' AND regexp_full_match(period_code, '[0-9]{6}')
            THEN TRY_CAST(substr(period_code, 5, 2) AS INTEGER)
    END AS period_month,
    CASE
        WHEN frequency_code = 'A' AND regexp_full_match(period_code, '[0-9]{4}')
            THEN CAST(TRY_STRPTIME(period_code || '0101', '%Y%m%d') AS DATE)
        WHEN frequency_code = 'M' AND regexp_full_match(period_code, '[0-9]{6}')
            THEN CAST(TRY_STRPTIME(period_code || '01', '%Y%m%d') AS DATE)
    END AS period_start_date,
    TRY_CAST(json_extract_string(record_json, '$.refYear') AS INTEGER) AS source_ref_year,
    TRY_CAST(json_extract_string(record_json, '$.refMonth') AS INTEGER) AS source_ref_month,
    json_extract_string(record_json, '$.reporterCode') AS reporter_code,
    json_extract_string(record_json, '$.reporterISO') AS reporter_iso,
    json_extract_string(record_json, '$.reporterDesc') AS reporter_description,
    json_extract_string(record_json, '$.flowCode') AS flow_code,
    json_extract_string(record_json, '$.flowDesc') AS flow_description,
    json_extract_string(record_json, '$.partnerCode') AS partner_code,
    json_extract_string(record_json, '$.partnerISO') AS partner_iso,
    json_extract_string(record_json, '$.partnerDesc') AS partner_description,
    json_extract_string(record_json, '$.partner2Code') AS partner2_code,
    json_extract_string(record_json, '$.partner2ISO') AS partner2_iso,
    json_extract_string(record_json, '$.partner2Desc') AS partner2_description,
    json_extract_string(record_json, '$.classificationSearchCode') AS classification_family_code,
    json_extract_string(record_json, '$.classificationCode') AS classification_version_code,
    TRY_CAST(json_extract_string(record_json, '$.isOriginalClassification') AS BOOLEAN)
        AS is_original_classification,
    json_extract_string(record_json, '$.cmdCode') AS commodity_code,
    json_extract_string(record_json, '$.cmdDesc') AS commodity_description,
    TRY_CAST(json_extract_string(record_json, '$.aggrLevel') AS SMALLINT)
        AS commodity_level,
    TRY_CAST(json_extract_string(record_json, '$.isLeaf') AS BOOLEAN) AS is_leaf,
    TRY_CAST(json_extract_string(record_json, '$.isAggregate') AS BOOLEAN) AS is_aggregate,
    json_extract_string(record_json, '$.cmdCode') = 'TOTAL' AS is_total_commodity,
    json_extract_string(record_json, '$.customsCode') AS customs_code,
    json_extract_string(record_json, '$.customsDesc') AS customs_description,
    json_extract_string(record_json, '$.motCode') AS mode_of_transport_code,
    json_extract_string(record_json, '$.motDesc') AS mode_of_transport_description,
    json_extract_string(record_json, '$.mosCode') AS mode_of_supply_code,
    TRY_CAST(json_extract_string(record_json, '$.primaryValue') AS DECIMAL(38,3))
        AS trade_value_usd,
    TRY_CAST(json_extract_string(record_json, '$.cifvalue') AS DECIMAL(38,3))
        AS cif_value_usd,
    TRY_CAST(json_extract_string(record_json, '$.fobvalue') AS DECIMAL(38,3))
        AS fob_value_usd,
    TRY_CAST(json_extract_string(record_json, '$.isReported') AS BOOLEAN) AS is_reported,
    TRY_CAST(json_extract_string(record_json, '$.legacyEstimationFlag') AS INTEGER)
        AS legacy_estimation_flag,
    source_file,
    source_record_ordinal,
    request_fingerprint,
    payload_sha256,
    retrieved_at_utc
FROM normalized;
