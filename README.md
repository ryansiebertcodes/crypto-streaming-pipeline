# crypto-streaming-pipeline

Real-time crypto trade ingestion with Kafka + PySpark — live OHLCV candles and Z-score anomaly detection, alerting via webhook.

## Architecture

```
Binance WebSocket
      ↓
  Producer (Python)
      ↓
  Kafka topic: raw-trades
      ↓
  Spark Structured Streaming
      ├── Tumbling 1-min window ──→ ohlcv_1m (PostgreSQL)
      └── Z-score (close/volume) ──→ anomalies (PostgreSQL)
                                            ↓
                                      Webhook alert
```

## Tech Stack

| Layer | Tool |
|---|---|
| Message broker | Apache Kafka (KRaft mode) |
| Stream processor | Apache Spark 4 (PySpark Structured Streaming) |
| Data source | Binance US WebSocket |
| Storage | PostgreSQL (Snowflake port available) |
| Infrastructure | Docker Compose |

## Prerequisites

- Docker Desktop
- Python 3.12+
- Java 17+ (`brew install openjdk@17`)

## Setup

```bash
# 1. Start Kafka and PostgreSQL
docker compose up -d

# 2. Install Python dependencies
make install

# 3. Start the Binance WebSocket producer
make producer

# 4. Start the Spark streaming job (separate terminal)
make stream
```

## Database Schema

**`ohlcv_1m`** — one row per symbol per minute

| Column | Type | Description |
|---|---|---|
| symbol | TEXT | Trading pair (e.g. BTCUSDT) |
| window_start | TIMESTAMPTZ | Start of the 1-minute window |
| open | NUMERIC | First price in window |
| high | NUMERIC | Highest price in window |
| low | NUMERIC | Lowest price in window |
| close | NUMERIC | Last price in window |
| volume | NUMERIC | Total quantity traded |

**`anomalies`** — detected anomaly events

| Column | Type | Description |
|---|---|---|
| symbol | TEXT | Trading pair |
| detected_at | TIMESTAMPTZ | When the anomaly was flagged |
| anomaly_type | TEXT | `price` or `volume` |
| z_score | NUMERIC | Deviation from rolling mean |
| ohlcv_ref | TIMESTAMPTZ | Reference candle window |

## Anomaly Detection

Z-score computed over a rolling lookback window of completed candles per symbol. Alerts fire when `abs(z_score) > 2.0` on close price or volume. Threshold is configurable in `config/settings.py`.
