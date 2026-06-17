# Next is src/spark_job.py — the heart of the pipeline. Before you write it, let me explain what it needs to do so you can approach it with a plan.

#   Spark Structured Streaming reads from Kafka as a continuous stream. Your job has three stages:

#   1. Read from Kafka
#   - Connect to localhost:9092, subscribe to raw-trades
#   - Each Kafka message comes in as bytes — you'll need to parse the JSON value back into columns (symbol, price, quantity, timestamp)

#   2. OHLCV aggregation
#   - Group by symbol + a tumbling 1-minute time window on timestamp
#   - Compute first(price) → open, max(price) → high, min(price) → low, last(price) → close, sum(quantity) → volume
#   - Write completed windows to ohlcv_1m in Postgres

#   3. Anomaly detection
#   - For each completed candle, compare its close and volume against a rolling Z-score of recent candles for that symbol
#   - If abs(z_score) > 2.0, write a row to anomalies and fire a webhook

#   Libraries you'll need:
#   pip install pyspark psycopg2-binary requests
#   pip freeze > requirements.txt
  
#   The PySpark entry point is SparkSession. The key concept to look up is readStream for Kafka input and writeStream with foreachBatch for writing to Postgres — foreachBatch gives
#   you a regular Pandas/Spark dataframe on each micro-batch which is where you'll run the Z-score logic.

#   Want to start writing it or have questions about any of these stages first?

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
# from pyspark.sql.functions import from_json, col, window, first, max, min, last, sum, to_timestamp
from pyspark.sql import functions as F
import psycopg2
from db import get_connection
from datetime import datetime, timezone
from alerts import send_alert


spark = SparkSession.builder.appName("crypto-stream").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2").getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "raw-trades").option("failOnDataLoss", "false").load()

schema = (StructType()
    .add("symbol", StringType())
    .add("price", StringType())
    .add("quantity", StringType())
    .add("timestamp", LongType())
)

parsed_df = (df.selectExpr("CAST(value AS STRING) as value")
    .select(F.from_json(F.col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("price", F.col("price").cast(DoubleType()))
    .withColumn("quantity", F.col("quantity").cast(DoubleType()))
)

windowed_df = (parsed_df
    .withColumn("price", F.col("price"))
    .withColumn("quantity", F.col("quantity"))
    .withColumn("trade_time", F.to_timestamp(F.col("timestamp") / 1000))
    .groupBy(F.window("trade_time", "1 minute"), "symbol")
    .agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("quantity").alias("volume")
    )
    .withColumn("window_start", F.col("window.start"))
)

def load_ohlcv_1m(conn, rows):
    insert_query = """
        INSERT INTO public.ohlcv_1m ( symbol, window_start, "open", high, low, "close", volume ) VALUES ( %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (symbol, window_start) DO UPDATE SET
            "open" = EXCLUDED."open",
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            "close" = EXCLUDED."close",
            volume = EXCLUDED.volume;
        """
    cur = conn.cursor()
    try:
        for row in rows:
            # print(row.symbol, row.price, row.quantity, row.timestamp)
            cur.execute(
                insert_query,
                (
                    row.symbol, 
                    row.window_start, 
                    row.open, 
                    row.high, 
                    row.low, 
                    row.close, 
                    row.volume
                ),
            )
        conn.commit()
        print(f"Loaded {len(rows)} candles into ohlcv_1m!")

    except Exception as e:
        print("Loading ohlcv_1m failed: ", e)
        conn.rollback()
    finally:
        cur.close()

def detect_anomalies(conn, rows):
    cur = conn.cursor()
    anomaly_count = 0
    try:
        for row in rows:
            cur.execute("""
                SELECT close, volume FROM ohlcv_1m
                WHERE symbol = %s
                ORDER BY window_start DESC
                LIMIT 20
            """, (row.symbol,))
            history = cur.fetchall()

            if len(history) < 5:
                continue

            closes = [float(r[0]) for r in history]
            volumes = [float(r[1]) for r in history]

            for metric, values, current in [
                ("price", closes, float(row.close)),
                ("volume", volumes, float(row.volume))
            ]:
                mean = sum(values) / len(values)
                std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5
                if std == 0:
                    continue
                z = (current - mean) / std
                if abs(z) > 2.0:
                    cur.execute("""
                        INSERT INTO anomalies (symbol, detected_at, anomaly_type, z_score, ohlcv_ref)
                        VALUES (%s, NOW(), %s, %s, %s)
                    """, (row.symbol, metric, z, row.window_start))
                    anomaly_count += 1
                    send_alert(row.symbol, metric, z, row.window_start)  # use metric not hardcoded

        conn.commit()
        print(f"Detected {anomaly_count} anomalies!")

    except Exception as e:
        print("Anomaly detection failed:", e)
        conn.rollback()
    finally:
        cur.close()

# Write to the postgreSQL db
def process_micro_batch(batch_df, batch_id):
    conn = get_connection()
    rows = batch_df.collect()
    load_ohlcv_1m(conn, rows)
    detect_anomalies(conn, rows)
    conn.close()
    # for row in rows:
        # print(row.symbol, row.price, row.quantity, row.timestamp)
        

# Attach the function to the stream
query = (
    windowed_df.writeStream.foreachBatch(process_micro_batch)
    .outputMode("update") # or "append"
    .option("checkpointLocation", "checkpoints/raw-trades")
    .start()
)

query.awaitTermination()

# if __name__ == "__main__":
#     url = "wss://stream.binance.us:9443/ws/btcusdt@trade"