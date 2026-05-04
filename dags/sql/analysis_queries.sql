-- 1. Évolution d'une devise par rapport à la base (EUR par défaut)
DROP VIEW IF EXISTS view_forex_evolution;
CREATE VIEW view_forex_evolution AS
SELECT 
    rate_date,
    base_currency,
    target_currency,
    rate,
    LAG(rate) OVER (PARTITION BY target_currency ORDER BY rate_date) as previous_rate,
    (rate - LAG(rate) OVER (PARTITION BY target_currency ORDER BY rate_date)) / LAG(rate) OVER (PARTITION BY target_currency ORDER BY rate_date) as daily_return
FROM clean_forex;

-- 2. Volatilité et variations extrêmes
DROP VIEW IF EXISTS view_forex_volatility;
CREATE VIEW view_forex_volatility AS
SELECT 
    target_currency,
    COUNT(*) as num_points,
    MIN(rate) as min_rate,
    MAX(rate) as max_rate,
    AVG(rate) as avg_rate,
    (STDDEV(rate) / AVG(rate)) * 100 as volatility_rel_pct, -- Volatilité relative en %
    (MAX(rate) - MIN(rate)) / MIN(rate) * 100 as max_spread_pct
FROM clean_forex
WHERE rate_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY target_currency
HAVING COUNT(*) > 1;

-- 3. Rapport de santé du pipeline
DROP VIEW IF EXISTS view_pipeline_health;
CREATE VIEW view_pipeline_health AS
SELECT 
    execution_date::DATE as run_day,
    status,
    SUM(lines_received) as total_received,
    SUM(lines_inserted) as total_inserted,
    SUM(lines_rejected) as total_rejected,
    ROUND((SUM(lines_inserted)::NUMERIC / NULLIF(SUM(lines_received), 0)) * 100, 2) as success_rate_pct
FROM logs_forex
GROUP BY 1, 2
ORDER BY 1 DESC;
