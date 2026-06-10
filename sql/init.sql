-- Run against PostgreSQL instance in Docker: psql -U ryansiebert -d climatiq_pipeline
-- Creates objects in the default public shcmea for ease


CREATE TABLE IF NOT EXISTS raw_ticks (
    symbol          TEXT,
    price           NUMERIC,
    quantity        NUMERIC,
    trade_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ohlcv_1m (
    symbol          TEXT,
    window_start    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    open            NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    volume          NUMERIC
);

CREATE TABLE IF NOT EXISTS anomalies (
    symbol          TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    anomaly_type    TEXT,
    z_score         NUMERIC,
    ohlcv_ref       TIMESTAMPTZ
);
