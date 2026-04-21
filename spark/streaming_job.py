"""
opentrend — streaming_job.py
------------------------------
Consumes GitHub events from Kafka using Spark Structured Streaming,
aggregates them in 10-minute tumbling windows, and writes results
to HBase.

Pipeline:
  Kafka (github-events)
    → Spark Structured Streaming
      → 10-min window aggregation
        → HBase: live_metrics  (top repos by event count)
        → HBase: repos         (enrichment — repo details)
"""

import os
import happybase
import logging
from datetime import datetime, timedelta
from urllib.parse import quote
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, to_timestamp, max as spark_max, unix_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType

load_dotenv()

# ── Configuration ─────────────────────────────────────────────
KAFKA_HOST  = os.getenv("KAFKA_HOST",  "hadoop-master:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
HBASE_HOST  = os.getenv("HBASE_HOST",  "hadoop-master")
HBASE_PORT  = int(os.getenv("HBASE_PORT", "9090"))
CHECKPOINT  = os.getenv(
    "STREAM_CHECKPOINT",
    "hdfs://hadoop-master:9000/user/root/opentrend/checkpoints/streaming-v3"
)

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("streaming_job")

# ── Kafka Message Schema ──────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("repo_name",  StringType(), True),
    StructField("actor",      StringType(), True),
    StructField("language",   StringType(), True),
    StructField("stars_count", LongType(), True),
    StructField("forks_count", LongType(), True),
    StructField("watchers_count", LongType(), True),
    StructField("created_at", StringType(), True),
])


def get_hbase_connection():
    """Open a HBase connection via Thrift."""
    return happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=30000
    )


def build_repo_key(repo_name: str) -> bytes:
    """
    Build a deterministic, collision-safe row key for repos table.
    We URL-encode the full repo name so slashes and special chars are preserved.
    """
    return quote(repo_name, safe="").encode("utf-8")


def week_start_from_window(window_start: str) -> str:
    """Convert a window start timestamp to Monday 00:00:00 week key format."""
    try:
        dt = datetime.fromisoformat(str(window_start).replace("Z", "+00:00"))
    except ValueError:
        return "unknown"

    monday = dt - timedelta(days=dt.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def write_live_events(batch_df, batch_id):
    """
    Write individual events to live_events table for real-time activity feed.
    Each row is one raw event — no window aggregation, just immediate write.
    """
    rows = batch_df.collect()
    if not rows:
        return

    log.info(f"Live events batch {batch_id}: writing {len(rows)} individual events")

    for i, row in enumerate(rows):
        connection = None
        try:
            connection = get_hbase_connection()
            table = connection.table("live_events")

            repo_name  = str(row["repo_name"])   if row["repo_name"]   else "unknown"
            event_type = str(row["event_type"])   if row["event_type"]   else "unknown"
            language  = str(row["language"])  if row["language"]   else "Unknown"
            repo_key  = build_repo_key(repo_name)

            ts_val = row["event_timestamp"]
            if ts_val:
                ts_str = ts_val.strftime("%Y%m%d%H%M%S")
            else:
                ts_str = "none"

            row_key = f"{ts_str}#{batch_id:04d}#{i:04d}#{repo_name}#{event_type}".encode("utf-8")

            table.put(row_key, {
                b"event:repo":     repo_name.encode("utf-8"),
                b"event:type":     event_type.encode("utf-8"),
                b"event:language": language.encode("utf-8"),
                b"event:key":     repo_key,
            })

            log.info(f"Live events batch {batch_id} row {i}: written OK")
        except Exception as e:
            import traceback
            log.error(f"Live events batch {batch_id} row {i}: write failed — {e}")
            log.error(traceback.format_exc())
        finally:
            if connection is not None:
                connection.close()


def write_to_hbase(batch_df, batch_id):
    """
    Called for every micro-batch by Spark Structured Streaming.
    Writes aggregated window results to:
      - live_metrics : top repos by event count per 10-min window
      - repos        : repo enrichment (language, name)
    """
    rows = batch_df.collect()

    if not rows:
        log.info(f"Batch {batch_id}: no rows to write")
        return

    log.info(f"Batch {batch_id}: writing {len(rows)} rows to HBase")

    connection = None
    try:
        connection = get_hbase_connection()
        live_table = connection.table("live_metrics")
        repos_table = connection.table("repos")

        for row in rows:
            repo_name = row["repo_name"] or "unknown"
            event_type = row["event_type"] or "unknown"
            event_count = str(row["event_count"])
            window_start = str(row["window_start"])
            language = row["language"] or "Unknown"
            stars_count = str(row["stars_count"] or 0)
            forks_count = str(row["forks_count"] or 0)
            watchers_count = str(row["watchers_count"] or 0)
            repo_key = build_repo_key(repo_name)
            weekly_week = week_start_from_window(window_start)

            # ── Write to live_metrics ──────────────────────────
            # Row key includes event_type to avoid overwriting rows for the
            # same repo/window when multiple event types exist.
            row_key = f"{window_start}#{repo_name}#{event_type}".encode("utf-8")

            live_table.put(row_key, {
                b"repo:name":       repo_name.encode("utf-8"),
                b"repo:event_type": event_type.encode("utf-8"),
                b"repo:language":   language.encode("utf-8"),
                b"repo:stars":      stars_count.encode("utf-8"),
                b"repo:forks":      forks_count.encode("utf-8"),
                b"repo:watchers":   watchers_count.encode("utf-8"),
                b"repo:key":        repo_key,
                b"metrics:week":    weekly_week.encode("utf-8"),
                b"metrics:count":   event_count.encode("utf-8"),
                b"metrics:window":  window_start.encode("utf-8"),
            })

            # ── Write to repos (enrichment) ────────────────────
            # Row key: URL-encoded repo_name (collision-safe)
            # Only write if repo not already known
            existing = repos_table.row(repo_key)

            if not existing:
                repos_table.put(repo_key, {
                    b"info:full_name": repo_name.encode("utf-8"),
                    b"info:language":  language.encode("utf-8"),
                })
                log.info(f"Batch {batch_id}: new repo stored → {repo_name}")

        log.info(f"Batch {batch_id}: HBase write complete")

    except Exception as e:
        log.error(f"Batch {batch_id}: HBase write failed — {e}")
    finally:
        if connection is not None:
            connection.close()


def main():
    log.info("opentrend Spark Streaming job starting...")
    log.info(f"  Kafka  : {KAFKA_HOST} / {KAFKA_TOPIC}")
    log.info(f"  HBase  : {HBASE_HOST}:{HBASE_PORT}")

    # ── Spark Session ──────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("opentrend-streaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")

    # ── Read Stream from Kafka ─────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_HOST)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON ────────────────────────────────────────────
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
            to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ssX")
        )
        .filter(
            col("event_timestamp").isNotNull() &
            col("repo_name").isNotNull() &
            col("event_type").isNotNull()
        )
    )

    # ── 10-minute Tumbling Window Aggregation ─────────────────
    # Groups by repo + event_type + language per 10-min window
    # event_count = number of events that repo received in that window
    windowed = (
        parsed
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "10 minutes"),
            col("repo_name"),
            col("event_type"),
            col("language"),
        )
        .agg(
            count("*").alias("event_count"),
            spark_max("stars_count").alias("stars_count"),
            spark_max("forks_count").alias("forks_count"),
            spark_max("watchers_count").alias("watchers_count"),
        )
        .select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("repo_name"),
            col("event_type"),
            col("language"),
            col("stars_count"),
            col("forks_count"),
            col("watchers_count"),
            col("event_count"),
        )
    )

    # ── Write to HBase via foreachBatch ───────────────────────
    query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(write_to_hbase)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Parallel live-events stream (no window aggregation) ─────────
    # Write individual raw events immediately for real-time activity feed.
    # Uses a separate checkpoint so it can run independently.
    live_events_stream = (
        parsed.writeStream
        .outputMode("append")
        .foreachBatch(write_live_events)
        .option("checkpointLocation", CHECKPOINT + "/live-events")
        .trigger(processingTime="10 seconds")
        .start()
    )

    log.info("Live events stream started")

    log.info("Streaming query started — waiting for events...")
    log.info("Press Ctrl+C to stop")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Stopping streaming job...")
        query.stop()
        live_events_stream.stop()
        spark.stop()


if __name__ == "__main__":
    main()