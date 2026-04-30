-- Schema initialization for ForexFlow project

-- 1. Raw layer: stores the exact response from the API
CREATE TABLE IF NOT EXISTS raw_forex (
    id SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    payload JSONB NOT NULL
);

-- 2. Clean layer: structured and validated data
-- We use a composite unique constraint to ensure idempotence
CREATE TABLE IF NOT EXISTS clean_forex (
    id SERIAL PRIMARY KEY,
    rate_date DATE NOT NULL,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    rate DECIMAL(18, 6) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (rate_date, base_currency, target_currency)
);

-- 3. Rejects layer: "cemetery" for invalid or corrupted data
CREATE TABLE IF NOT EXISTS rejects_forex (
    id SERIAL PRIMARY KEY,
    raw_id INTEGER REFERENCES raw_forex(id),
    reason TEXT,
    payload JSONB,
    rejected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Alerts layer: stores detected anomalies based on variation thresholds
CREATE TABLE IF NOT EXISTS alerts_forex (
    id SERIAL PRIMARY KEY,
    currency_pair VARCHAR(10) NOT NULL,
    old_rate DECIMAL(18, 6),
    new_rate DECIMAL(18, 6),
    variation_pct DECIMAL(10, 4),
    alert_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Logs layer: technical monitoring of the pipeline runs
CREATE TABLE IF NOT EXISTS logs_forex (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255),
    status VARCHAR(50),
    lines_received INTEGER DEFAULT 0,
    lines_valid INTEGER DEFAULT 0,
    lines_rejected INTEGER DEFAULT 0,
    lines_inserted INTEGER DEFAULT 0,
    execution_date TIMESTAMP WITH TIME ZONE,
    logged_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_clean_date ON clean_forex(rate_date);
CREATE INDEX IF NOT EXISTS idx_clean_pair ON clean_forex(base_currency, target_currency);
