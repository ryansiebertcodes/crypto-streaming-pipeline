# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time crypto streaming pipeline. Ingests live trade ticks from Binance WebSocket, aggregates into 1-minute OHLCV candles via Spark Structured Streaming, and runs Z-score anomaly detection on price and volume — firing webhook alerts on significant deviations.

**Portfolio story:** Streaming ingestion → windowed aggregation → stateful analytics → alerting, all in one pipeline.

## Tech Stack

| Layer | Tool |
|---|---|
| Message broker | Apache Kafka + Zookeeper |
| Stream processor | Apache Spark (PySpark Structured Streaming) |
| Data source | Binance WebSocket (no API key required) |
| Storage | PostgreSQL (primary), Snowflake (port target) |
| Infrastructure | Docker Compose |
| Alerting | Webhook (Discord or HTTP endpoint) |

## Running the Project

```bash
docker-compose up -d          # Start Kafka, Zookeeper, Spark, PostgreSQL
make producer                 # Start Binance WebSocket → Kafka producer
make stream                   # Start Spark Structured Streaming job
make dashboard                # Launch Streamlit dashboard (if added)
docker-compose down           # Tear down all services
```

## Data Flow

```
Binance WebSocket
      ↓
  Producer (Python)
      ↓
  Kafka topic: raw-trades
      ↓
  Spark Structured Streaming
      ├── Tumbling 1-min window ──→ ohlcv_1m table (PostgreSQL)
      └── Z-score (close/volume) ──→ anomalies table (PostgreSQL)
                                            ↓
                                      Webhook alert
```

## Database Schema

**`raw_ticks`** — optional ring buffer, drop after N days
- `symbol`, `price`, `quantity`, `trade_timestamp`

**`ohlcv_1m`** — one row per symbol per minute
- `symbol`, `window_start`, `open`, `high`, `low`, `close`, `volume`

**`anomalies`** — detected anomaly events
- `symbol`, `detected_at`, `anomaly_type` (price | volume), `z_score`, `ohlcv_ref`

## Repository Structure

```
docker-compose.yml
Makefile
requirements.txt
src/
  producer.py          # Binance WS → Kafka
  spark_job.py         # Structured Streaming: OHLCV + anomaly detection
  sinks/
    base.py            # BaseSink abstract class
    postgres.py        # PostgresSink
    snowflake.py       # SnowflakeSink (port target)
  alerts.py            # Webhook alert dispatch
sql/
  init.sql             # Schema creation
config/
  settings.py          # Kafka brokers, DB connection, thresholds
tests/
  test_transformations.py
```

## Sink Abstraction (Snowflake Port)

`BaseSink` defines the interface. `PostgresSink` is the default. To port to Snowflake, set `SINK=snowflake` in environment — `SnowflakeSink` implements the same interface against a Snowflake trial account.

## Anomaly Detection Logic

Z-score computed over a rolling lookback window of completed candles per symbol:
- Flag if `abs(z_score) > 2.0` on close price or volume
- Threshold configurable in `config/settings.py`
