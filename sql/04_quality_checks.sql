WITH checks AS (
    SELECT 'required_staging_fields' AS check_name,
        count(*) FILTER (WHERE reporter_code IS NULL OR frequency_code IS NULL
            OR period_code IS NULL OR flow_code IS NULL OR commodity_code IS NULL
            OR partner_code IS NULL OR trade_value_usd IS NULL) AS failure_count,
        'required staging values must be populated' AS detail
    FROM staging.trade_observation
    UNION ALL
    SELECT 'valid_periods', count(*) FILTER (
        WHERE frequency_code NOT IN ('A', 'M') OR period_year IS NULL
          OR period_start_date IS NULL
          OR (frequency_code = 'A' AND period_month IS NOT NULL)
          OR (frequency_code = 'M' AND (period_month NOT BETWEEN 1 AND 12))
    ), 'annual and monthly period fields must be internally consistent'
    FROM staging.trade_observation
    UNION ALL
    SELECT 'source_period_consistency', count(*) FILTER (
        WHERE source_ref_year <> period_year
           OR (frequency_code = 'M' AND source_ref_month <> period_month)
    ), 'source reference fields must agree with the normalized period'
    FROM staging.trade_observation
    UNION ALL
    SELECT 'fact_grain_unique', count(*), 'fact business keys must be unique'
    FROM (
        SELECT 1
        FROM core.fact_trade
        GROUP BY reporter_code, type_code, frequency_code, period_code, flow_code,
            classification_version_code, commodity_code, partner_code, partner2_code,
            customs_code, mode_of_transport_code, mode_of_supply_code
        HAVING count(*) > 1
    ) duplicates
    UNION ALL
    SELECT 'partner_dimension_unique', count(*), 'partner codes must map once'
    FROM (SELECT partner_code FROM core.dim_partner GROUP BY partner_code HAVING count(*) > 1)
    UNION ALL
    SELECT 'product_dimension_unique', count(*), 'product natural keys must map once'
    FROM (SELECT classification_version_code, commodity_code FROM core.dim_product
          GROUP BY ALL HAVING count(*) > 1)
    UNION ALL
    SELECT 'partner_referential_integrity', count(*), 'every fact partner must resolve'
    FROM core.fact_trade f LEFT JOIN core.dim_partner d USING (partner_code)
    WHERE d.partner_code IS NULL
    UNION ALL
    SELECT 'product_referential_integrity', count(*), 'every fact product must resolve'
    FROM core.fact_trade f LEFT JOIN core.dim_product d
      USING (classification_version_code, commodity_code)
    WHERE d.commodity_code IS NULL
    UNION ALL
    SELECT 'staging_fact_count', abs(
        (SELECT count(*) FROM staging.trade_observation)
        - (SELECT count(*) FROM core.fact_trade)
    ), 'staging and fact counts must agree'
    UNION ALL
    SELECT 'annual_marts_only',
        (SELECT count(*) FROM analytics.chapter_trade_balance b
         WHERE b.period_year IS NULL),
        'annual marts must contain valid annual periods'
)
SELECT check_name, CAST(failure_count AS BIGINT), detail FROM checks ORDER BY check_name;
