"""
opentrend — batch_job.py
--------------------------
Reads GH Archive .json.gz files from HDFS, computes weekly
metrics per repo, and writes results to HBase.

Run manually or triggered by scheduler.py weekly.

Pipeline (TP1 MapReduce logic, rewritten in PySpark TP2 style):
  HDFS /user/root/gharchive/*.json.gz
    → PySpark (read + aggregate)
      → HBase: weekly_metrics  (star velocity per repo)
      → HBase: ml_predictions  (simple trending score)
"""

import os
import sys
import glob
import happybase
import logging
from pathlib import Path
from urllib.parse import quote
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum,
    when, lit, desc, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
load_dotenv(BASE_DIR / ".env")

# ── Configuration ─────────────────────────────────────────────
HBASE_HOST  = os.getenv("HBASE_HOST",  "hadoop-master")
HBASE_PORT  = int(os.getenv("HBASE_PORT", "9090"))
HDFS_HOST   = os.getenv("HDFS_HOST",   "hadoop-master:9000")
LOCAL_INPUT_DIR = os.getenv("LOCAL_GHARCHIVE_DIR", "/data/gharchive")
LOCAL_INPUT = f"{LOCAL_INPUT_DIR}/*.json.gz"
HDFS_INPUT  = f"hdfs://{HDFS_HOST}/user/root/gharchive/*.json.gz"

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("batch_job")


def resolve_input_path() -> str:
    """
    Prefer mounted local GH Archive files when available.
    Fallback to HDFS path for cluster-managed datasets.
    """
    local_files = glob.glob(LOCAL_INPUT)
    if local_files:
        log.info(f"Found {len(local_files)} local GH Archive file(s) in {LOCAL_INPUT_DIR}")
        return LOCAL_INPUT

    log.warning("No local GH Archive files found, falling back to HDFS input")
    return HDFS_INPUT


def get_hbase_connection():
    return happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=10000
    )


def build_repo_key(repo_name: str) -> bytes:
    """Build a deterministic, collision-safe repo key used across HBase tables."""
    return quote(repo_name, safe="").encode("utf-8")

def enrich_with_language(df_top, spark):
    """
    Call GitHub API to get language for each top repo.
    Only enriches top 200 repos to stay within rate limits.
    """
    import requests

    repo_names = [row["repo_name"] for row in df_top.collect()]
    log.info(f"Fetching language for {len(repo_names)} repos...")

    language_map = {}
    headers = {
    "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
    "Accept": "application/vnd.github+json"
}

    for repo in repo_names:
        try:
            r = requests.get(
                f"https://api.github.com/repos/{repo}",
                headers=headers, timeout=5)
            if r.status_code == 200:
                language_map[repo] = r.json().get("language") or "Unknown"
                print(f"Enriched {repo} with language: {language_map[repo]}")
            else:
                language_map[repo] = "Unknown"
                print(f"Failed to fetch {repo}: {r.status_code} - {r.text}")
        except Exception:
            language_map[repo] = "Unknown"

    log.info("Language enrichment complete")
    return language_map

def write_weekly_metrics(df_weekly,language_map=None):
    """
    Write weekly aggregated metrics to HBase weekly_metrics table.

    Row key: week_date#repo_name
    Column families: repo (name, language) / stats (stars, forks, velocity)
    """
    rows = df_weekly.collect()
    log.info(f"Writing {len(rows)} rows to weekly_metrics...")

    connection = get_hbase_connection()
    table = connection.table("weekly_metrics")

    for row in rows:
        repo_name  = row["repo_name"]  or "unknown"
        week       = row["week"]       or "unknown"
        stars      = str(row["star_count"]  or 0)
        forks      = str(row["fork_count"]  or 0)
        velocity   = str(row["velocity"]    or 0)
        repo_key   = build_repo_key(repo_name)

        row_key = f"{week}#{repo_name}".encode("utf-8")
        language = language_map.get(repo_name, "Unknown") if language_map else "Unknown"
        table.put(row_key, {
            b"repo:name":       repo_name.encode("utf-8"),
            b"repo:language":   language.encode("utf-8"),
            b"repo:key":        repo_key,
            b"stats:stars":     stars.encode("utf-8"),
            b"stats:forks":     forks.encode("utf-8"),
            b"stats:velocity":  velocity.encode("utf-8"),
            b"stats:week":      week.encode("utf-8"),
        })

    connection.close()
    log.info("weekly_metrics write complete")


def write_ml_predictions(df_predictions):
    """
    Write ML trending predictions to HBase ml_predictions table.

    Simple scoring model:
      - velocity > 10  → high probability trending
      - velocity > 5   → medium
      - else           → low

    Row key: repo_name
    Column families: repo (name) / ml (score, cluster, predicted_growth)
    """
    rows = df_predictions.collect()
    log.info(f"Writing {len(rows)} rows to ml_predictions...")

    connection = get_hbase_connection()
    table = connection.table("ml_predictions")

    for row in rows:
        repo_name = row["repo_name"] or "unknown"
        velocity  = row["velocity"]  or 0
        stars     = row["star_count"] or 0
        repo_key  = build_repo_key(repo_name)

        # Simple trending probability score (0.0 to 1.0)
        if velocity > 10:
            probability = 0.9
            cluster     = "Rocket repo"
        elif velocity > 5:
            probability = 0.6
            cluster     = "Rising repo"
        elif velocity > 2:
            probability = 0.35
            cluster     = "Steady grower"
        else:
            probability = 0.1
            cluster     = "Low activity"

        # Predicted growth = velocity * 1.2 (simple linear projection)
        predicted_growth = int(velocity * 1.2)

        row_key = repo_name.encode("utf-8")

        table.put(row_key, {
            b"repo:name":            repo_name.encode("utf-8"),
            b"repo:key":             repo_key,
            b"ml:probability":       str(probability).encode("utf-8"),
            b"ml:cluster":           cluster.encode("utf-8"),
            b"ml:predicted_growth":  str(predicted_growth).encode("utf-8"),
            b"ml:velocity":          str(velocity).encode("utf-8"),
        })

    connection.close()
    log.info("ml_predictions write complete")


def main():
    input_path = resolve_input_path()

    log.info("opentrend Spark Batch job starting...")
    log.info(f"  Input path : {input_path}")
    log.info(f"  HBase      : {HBASE_HOST}:{HBASE_PORT}")

    # ── Create Spark Session ───────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("opentrend-batch")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session created")

    # ── Read GH Archive files from HDFS ───────────────────────
    # GH Archive files are JSON (one event per line) inside .gz
    # Spark reads them natively — same as TP1 HDFS file read
    log.info("Reading GH Archive files...")
    try:
        raw = spark.read.option("badRecordsPath", "/tmp/bad_records").json(input_path)
    except Exception as e:
        log.error(f"Failed to read GH Archive input: {e}")
        log.error(f"Checked local path: {LOCAL_INPUT}")
        log.error(f"Checked HDFS path:  {HDFS_INPUT}")
        log.warning("Attempting to read valid files only...")
        # Try reading with permissive mode to skip corrupted files
        try:
            raw = spark.read.option("badRecordsPath", "/tmp/bad_records").option("mode", "PERMISSIVE").json(input_path)
            log.info(f"Recovered: read {raw.count()} valid events after skipping corrupted files")
        except Exception as e2:
            log.error(f"Unable to recover from read failure: {e2}")
            sys.exit(1)

    log.info(f"Total events loaded: {raw.count()}")

    # ── Filter relevant events ─────────────────────────────────
    relevant = raw.filter(
        col("type").isin(
            "WatchEvent", "ForkEvent", "PushEvent", "CreateEvent"
        )
    )

    # ── Extract week from created_at ───────────────────────────
    # Use substring to get YYYY-MM-DD, then truncate to week start
    from pyspark.sql.functions import date_trunc, to_timestamp, substring

    events = relevant.select(
        col("type").alias("event_type"),
        col("repo.name").alias("repo_name"),
        col("actor.login").alias("actor"),
        col("created_at"),
        date_trunc(
            "week",
            to_timestamp(col("created_at"))
        ).cast("string").alias("week")
    )

    # ── Weekly aggregation per repo ────────────────────────────
    # Count stars (WatchEvents) and forks per repo per week
    # Same MapReduce logic as TP1 WordCount but for repos
    weekly = (
        events
        .groupBy("week", "repo_name")
        .agg(
            count(
                when(col("event_type") == "WatchEvent", 1)
            ).alias("star_count"),
            count(
                when(col("event_type") == "ForkEvent", 1)
            ).alias("fork_count"),
            count("*").alias("total_events")
        )
        .orderBy(desc("star_count"))
    )

    # ── Compute velocity ───────────────────────────────────────
    # Velocity = star_count (simple: stars gained this week)
    # In a real system you'd compare to previous week
    weekly_with_velocity = weekly.withColumn(
        "velocity", col("star_count")
    )

    log.info("Weekly aggregation complete")
    weekly_with_velocity.show(10, truncate=False)

    # ── Get top 200 repos by velocity ─────────────────────────

    top_repos = weekly_with_velocity.orderBy(
        desc("velocity")
    ).limit(200)
    language_map = enrich_with_language(top_repos, spark)

    # ── Write to HBase ─────────────────────────────────────────
    write_weekly_metrics(weekly_with_velocity, language_map)

    # ── ML predictions — top 200 repos only ───────────────────
  

    write_ml_predictions(top_repos)

    log.info("Batch job complete")
    spark.stop()


if __name__ == "__main__":
    main()