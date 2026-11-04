CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE TABLE analytics.chapter_trade_balance AS
SELECT
    f.period_year,
    f.classification_version_code,
    f.commodity_code,
    p.commodity_description,
    MAX(f.trade_value_usd) FILTER (WHERE f.flow_code = 'M') AS imports_usd,
    MAX(f.trade_value_usd) FILTER (WHERE f.flow_code = 'X') AS exports_usd,
    MAX(f.trade_value_usd) FILTER (WHERE f.flow_code = 'X')
        - MAX(f.trade_value_usd) FILTER (WHERE f.flow_code = 'M')
        AS trade_balance_usd
FROM core.fact_trade f
JOIN core.dim_product p USING (classification_version_code, commodity_code)
WHERE f.frequency_code = 'A'
  AND p.commodity_level = 2
  AND f.partner_code = '0'
  AND f.partner2_code = '0'
  AND f.customs_code = 'C00'
  AND f.mode_of_transport_code = '0'
  AND f.mode_of_supply_code = '0'
GROUP BY ALL;

CREATE OR REPLACE TABLE analytics.partner_trade_analysis AS
WITH world AS (
    SELECT
        reporter_code, period_year, flow_code, classification_version_code,
        commodity_code, trade_value_usd AS world_trade_value_usd
    FROM core.fact_trade
    WHERE frequency_code = 'A'
      AND partner_code = '0'
      AND partner2_code = '0'
      AND customs_code = 'C00'
      AND mode_of_transport_code = '0'
      AND mode_of_supply_code = '0'
), partners AS (
    SELECT f.*, p.commodity_description, d.partner_description, d.partner_scope
    FROM core.fact_trade f
    JOIN core.dim_product p USING (classification_version_code, commodity_code)
    JOIN core.dim_partner d USING (partner_code)
    WHERE f.frequency_code = 'A'
      AND p.commodity_level = 2
      AND f.partner_code <> '0'
      AND f.partner2_code = '0'
      AND f.customs_code = 'C00'
      AND f.mode_of_transport_code = '0'
      AND f.mode_of_supply_code = '0'
)
SELECT
    p.period_year,
    p.flow_code,
    p.classification_version_code,
    p.commodity_code,
    p.commodity_description,
    p.partner_code,
    p.partner_description,
    p.partner_scope,
    p.trade_value_usd,
    w.world_trade_value_usd,
    p.trade_value_usd / NULLIF(w.world_trade_value_usd, 0) AS share_of_world,
    RANK() OVER (
        PARTITION BY p.period_year, p.flow_code, p.classification_version_code,
            p.commodity_code
        ORDER BY p.trade_value_usd DESC
    ) AS partner_rank
FROM partners p
LEFT JOIN world w
  ON p.reporter_code = w.reporter_code
 AND p.period_year = w.period_year
 AND p.flow_code = w.flow_code
 AND p.classification_version_code = w.classification_version_code
 AND p.commodity_code = w.commodity_code;

CREATE OR REPLACE TABLE analytics.chapter_yoy AS
WITH annual AS (
    SELECT
        period_year,
        flow_code,
        classification_version_code,
        commodity_code,
        trade_value_usd,
        LAG(period_year) OVER grain AS prior_year,
        LAG(trade_value_usd) OVER grain AS prior_value_usd
    FROM core.fact_trade
    JOIN core.dim_product USING (classification_version_code, commodity_code)
    WHERE frequency_code = 'A'
      AND commodity_level = 2
      AND partner_code = '0'
      AND partner2_code = '0'
      AND customs_code = 'C00'
      AND mode_of_transport_code = '0'
      AND mode_of_supply_code = '0'
    WINDOW grain AS (
        PARTITION BY flow_code, classification_version_code, commodity_code
        ORDER BY period_year
    )
)
SELECT
    classification_version_code,
    commodity_code,
    flow_code,
    period_year AS current_year,
    CASE WHEN prior_year = period_year - 1 THEN prior_year END AS prior_year,
    trade_value_usd AS current_value_usd,
    CASE WHEN prior_year = period_year - 1 THEN prior_value_usd END AS prior_value_usd,
    CASE WHEN prior_year = period_year - 1
        THEN trade_value_usd - prior_value_usd END AS absolute_change_usd,
    CASE WHEN prior_year = period_year - 1 AND prior_value_usd <> 0
        THEN (trade_value_usd - prior_value_usd) / prior_value_usd END
        AS percentage_change
FROM annual;
