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
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("crypto-stream").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2").getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "raw-trades").load()
parsed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


def process_micro_batch(batch_df, batch_id):
    # .collect() pulls data as a list of Row objects to the driver
    rows = batch_df.collect()
    
    for row in rows:
        # Access column values
      data = json.loads(row.value)
      print(data['symbol'], data['price'], data['quantity'], data['timestamp'])


# Attach the function to the stream
query = (
    parsed_df.writeStream.foreachBatch(process_micro_batch)
    .outputMode("update") # or "append"
    .option("checkpointLocation", "checkpoints/raw-trades")
    .start()
)

query.awaitTermination()

# if __name__ == "__main__":
#     url = "wss://stream.binance.us:9443/ws/btcusdt@trade"