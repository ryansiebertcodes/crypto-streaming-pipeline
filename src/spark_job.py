# Standard library
import os

# Third-party
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Local
from db import get_connection
from sinks.postgres import PostgresSink
from sinks.snowflake import SnowflakeSink

schema = (StructType()
    .add("symbol", StringType())
    .add("price", StringType())
    .add("quantity", StringType())
    .add("timestamp", LongType())
)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("crypto-stream").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2").getOrCreate()
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "raw-trades").option("failOnDataLoss", "false").load()

    parsed_df = (df.selectExpr("CAST(value AS STRING) as value")
        .select(F.from_json(F.col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("quantity", F.col("quantity").cast(DoubleType()))
    )

    windowed_df = (parsed_df
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

    SINK = os.environ.get("SINK", "postgres")
    sink = PostgresSink() if SINK == "postgres" else SnowflakeSink()

    # Write to the postgreSQL db
    def process_micro_batch(batch_df, batch_id):
        conn = get_connection()
        rows = batch_df.collect()

        sink.write_ohlcv(conn, rows)
        sink.write_anomaly(conn, rows)
        conn.close()       

    # Attach the function to the stream
    query = (
        windowed_df.writeStream.foreachBatch(process_micro_batch)
        .outputMode("update") # or "append"
        .option("checkpointLocation", "checkpoints/raw-trades")
        .start()
    )

    query.awaitTermination()

