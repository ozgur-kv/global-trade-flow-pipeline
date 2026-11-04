WITH expected AS (
    SELECT * FROM (VALUES
        (2023, '84', 179230000000.000::DECIMAL(38,3),
            277470000000.000::DECIMAL(38,3), 98240000000.000::DECIMAL(38,3)),
        (2024, '84', 174710000000.000::DECIMAL(38,3),
            271790000000.000::DECIMAL(38,3), 97080000000.000::DECIMAL(38,3)),
        (2023, '85', 225960000000.000::DECIMAL(38,3),
            192970000000.000::DECIMAL(38,3), -32990000000.000::DECIMAL(38,3)),
        (2024, '85', 210230000000.000::DECIMAL(38,3),
            181890000000.000::DECIMAL(38,3), -28340000000.000::DECIMAL(38,3)),
        (2023, '87', 160690000000.000::DECIMAL(38,3),
            297610000000.000::DECIMAL(38,3), 136920000000.000::DECIMAL(38,3)),
        (2024, '87', 148610000000.000::DECIMAL(38,3),
            285780000000.000::DECIMAL(38,3), 137170000000.000::DECIMAL(38,3))
    ) AS values(period_year, commodity_code, imports_usd, exports_usd, balance_usd)
), metric_failures AS (
    SELECT e.period_year, e.commodity_code
    FROM expected e
    LEFT JOIN analytics.chapter_trade_balance a
      USING (period_year, commodity_code)
    WHERE a.period_year IS NULL
       OR abs(a.imports_usd - e.imports_usd) > 5000001
       OR abs(a.exports_usd - e.exports_usd) > 5000001
       OR abs(a.trade_balance_usd - e.balance_usd) > 5000001
), world_counts AS (
    SELECT partition_id, count(*) FILTER (WHERE partner_code = '0') AS world_rows
    FROM staging.trade_observation GROUP BY partition_id
)
SELECT 'expected_partitions' AS check_name,
    abs((SELECT count(*) FROM staging.source_partitions) - 12)::BIGINT AS failure_count,
    'current dataset must contain 12 manifest partitions' AS detail
UNION ALL
SELECT 'expected_records',
    abs((SELECT count(*) FROM core.fact_trade) - 2412)::BIGINT,
    'current dataset must contain 2,412 fact rows'
UNION ALL
SELECT 'one_world_per_partition', count(*)::BIGINT,
    'each current partition must contain exactly one World row'
FROM world_counts WHERE world_rows <> 1
UNION ALL
SELECT 'notebook04_world_values', count(*)::BIGINT,
    'rounded Notebook 04 metrics must reconcile within USD 5,000,001'
FROM metric_failures
ORDER BY check_name;
