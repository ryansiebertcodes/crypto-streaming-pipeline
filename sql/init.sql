-- Run against PostgreSQL instance in Docker: psql -U ryansiebert -d climatiq_pipeline
-- Creates objects in the default public shcmea for ease

-- raw_ticks is an append-only log of every trade event. Each tick is unique by nature
CREATE TABLE IF NOT EXISTS raw_ticks (
    id              SERIAL PRIMARY KEY,
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
    volume          NUMERIC,
    CONSTRAINT pk_ohlcv_1m PRIMARY KEY (symbol, window_start)
);

-- anomalies is an append-only log of every trade event. Each anomaly record is unique by nature
CREATE TABLE IF NOT EXISTS anomalies (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    anomaly_type    TEXT,
    z_score         NUMERIC,
    ohlcv_ref       TIMESTAMPTZ
);
