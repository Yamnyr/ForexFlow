-- 1. Évolution d'une devise par rapport à la base (EUR par défaut)
-- Permet de visualiser la tendance temporelle
CREATE OR REPLACE VIEW view_forex_evolution AS
SELECT 
    rate_date,
    base_currency,
    target_currency,
    rate,
    LAG(rate) OVER (PARTITION BY target_currency ORDER BY rate_date) as previous_rate,
    (rate - LAG(rate) OVER (PARTITION BY target_currency ORDER BY rate_date)) / LAG(rate) OVER (PARTITION BY target_currency ORDER BY rate_date) as daily_return
FROM clean_forex;

-- 2. Volatilité et variations extrêmes
-- Identifie les devises les plus instables sur les 30 derniers jours
CREATE OR REPLACE VIEW view_forex_volatility AS
SELECT 
    target_currency,
    COUNT(*) as num_points,
    MIN(rate) as min_rate,
    MAX(rate) as max_rate,
    AVG(rate) as avg_rate,
    STDDEV(rate) as volatility_std,
    (MAX(rate) - MIN(rate)) / MIN(rate) as max_spread_pct
FROM clean_forex
WHERE rate_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY target_currency
HAVING COUNT(*) > 1;

-- 3. Top des variations quotidiennes (Requête ad-hoc)
-- Utile pour identifier rapidement les mouvements de marché importants
-- SELECT * FROM view_forex_evolution 
-- WHERE ABS(daily_return) > 0.02 
-- ORDER BY ABS(daily_return) DESC;

-- 4. Rapport de santé du pipeline
-- Permet de vérifier les rejets et les taux de succès
CREATE OR REPLACE VIEW view_pipeline_health AS
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
