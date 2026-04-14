"""
opentrend — streaming_job.py
------------------------------
Consumes GitHub events from Kafka using Spark Structured Streaming,
aggregates them in 10-minute tumbling windows, and writes results
to HBase.

Pipeline (same as TP2 + TP3 combined):
  Kafka (github-events)
    → Spark Structured Streaming
      → 10-min window aggregation
        → HBase: live_metrics  (top repos by event count)
        → HBase: events_stream (raw event feed for dashboard)
"""

import os
import json
import happybase
import logging
from datetime import datetime
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, to_timestamp,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType
)

load_dotenv()

# ── Configuration ─────────────────────────────────────────────
KAFKA_HOST   = os.getenv("KAFKA_HOST",  "hadoop-master:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "github-events")
HBASE_HOST   = os.getenv("HBASE_HOST",  "hadoop-master")
HBASE_PORT   = int(os.getenv("HBASE_PORT", "9090"))
CHECKPOINT   = "/tmp/spark-checkpoint"

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("streaming_job")

# ── Schema of messages coming from Kafka ─────────────────────
# Matches exactly what producer.py publishes
EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("repo_name",  StringType(), True),
    StructField("actor",      StringType(), True),
    StructField("created_at", StringType(), True),
])


def get_hbase_connection():
    """Open a HBase connection via Thrift (same server as TP4)."""
    return happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=30000
    )


def write_live_metrics(batch_df, batch_id):
    """
    Called for every micro-batch by Spark Structured Streaming.

    Takes the aggregated window results and writes them to HBase:
    - live_metrics  : top repos by event count in the last 10 min
    - events_stream : individual raw events for the live feed panel
    """
    # Collect results from this micro-batch to the driver
    rows = batch_df.collect()

    if not rows:
        log.info(f"Batch {batch_id}: no rows to write")
        return

    log.info(f"Batch {batch_id}: writing {len(rows)} rows to HBase")

    try:
        connection = get_hbase_connection()

        # ── Write to live_metrics ──────────────────────────────
        # HBase table structure (defined in Step 2):
        #   live_metrics
        #     column family: repo   → name, event_type
        #     column family: stats  → count, window_start
        #
        # Row key: window_start_timestamp#repo_name
        # (same pattern as TP4 sales_ledger row keys)

        live_table = connection.table("live_metrics")

        for row in rows:
            repo_name    = row["repo_name"]   if row["repo_name"]   else "unknown"
            event_type   = row["event_type"]  if row["event_type"]  else "unknown"
            event_count  = str(row["event_count"])
            window_start = str(row["window_start"])

            # Row key: combines window + repo for unique identification
            row_key = f"{window_start}#{repo_name}".encode("utf-8")

            live_table.put(row_key, {
                b"repo:name":        repo_name.encode("utf-8"),
                b"repo:event_type":  event_type.encode("utf-8"),
                b"stats:count":      event_count.encode("utf-8"),
                b"stats:window":     window_start.encode("utf-8"),
            })

        # ── Write to events_stream ─────────────────────────────
        # Stores individual raw events for the Live Feed panel
        # Row key: timestamp#event_id

        stream_table = connection.table("events_stream")

        for row in rows:
            repo_name  = row["repo_name"]  if row["repo_name"]  else "unknown"
            event_type = row["event_type"] if row["event_type"] else "unknown"
            actor      = row["actor"]      if row["actor"]      else "unknown"
            created_at = row["created_at"] if row["created_at"] else ""
            event_id   = row["event_id"]   if row["event_id"]   else "0"

            row_key = f"{created_at}#{event_id}".encode("utf-8")

            stream_table.put(row_key, {
                b"event:type":       event_type.encode("utf-8"),
                b"event:repo":       repo_name.encode("utf-8"),
                b"event:actor":      actor.encode("utf-8"),
                b"event:created_at": created_at.encode("utf-8"),
            })

        connection.close()
        log.info(f"Batch {batch_id}: HBase write complete")

    except Exception as e:
        log.error(f"Batch {batch_id}: HBase write failed — {e}")


def main():
    log.info("opentrend Spark Streaming job starting...")
    log.info(f"  Kafka  : {KAFKA_HOST} / {KAFKA_TOPIC}")
    log.info(f"  HBase  : {HBASE_HOST}:{HBASE_PORT}")

    # ── Create Spark Session ───────────────────────────────────
    # Same pattern as TP2 SparkSession.builder()
    spark = (
        SparkSession.builder
        .appName("opentrend-streaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT)
        # Kafka connector package
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")

    # ── Read stream from Kafka ─────────────────────────────────
    # Same as TP3 SparkKafkaWordCount:
    #   spark.readStream().format("kafka")...
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_HOST)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON messages ────────────────────────────────────
    # Kafka delivers messages as binary — cast value to string
    # then parse the JSON using our schema
    parsed = (
        raw_stream
        .select(
            from_json(
                col("value").cast("string"),
                EVENT_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_timestamp",
            to_timestamp(col("created_at"))
        )
    )

    # ── 10-minute tumbling window aggregation ──────────────────
    # Groups events by repo_name and event_type within each
    # 10-minute window — this is the core of the stream processing
    #
    # Equivalent to the TP2 wordcount groupBy but for repos
    windowed = (
        parsed
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "10 minutes"),
            col("repo_name"),
            col("event_type"),
            col("actor"),
            col("event_id"),
            col("created_at"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("repo_name"),
            col("event_type"),
            col("actor"),
            col("event_id"),
            col("created_at"),
            col("event_count"),
        )
    )

    # ── Write to HBase via foreachBatch ───────────────────────
    # foreachBatch is the bridge between Spark Streaming
    # and any custom sink (HBase in our case)
    query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(write_live_metrics)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("Streaming query started — waiting for events...")
    log.info("Press Ctrl+C to stop")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopping streaming job...")
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()