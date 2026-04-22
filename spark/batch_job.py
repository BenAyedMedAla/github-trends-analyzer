"""
opentrend — batch_job.py
--------------------------
Reads GH Archive .json.gz files from HDFS, computes weekly
metrics per repo, and writes results to HBase.

Run manually or triggered by Airflow DAG weekly.

Pipeline:
  HDFS /user/root/gharchive/*.json.gz
    → PySpark (read + aggregate)
      → HBase: weekly_metrics  (star velocity per repo)

Features:
  - Incremental processing: skips already-processed weeks
  - Actual velocity calculation: compares to previous week
  - Bulk HBase writes for efficiency
  - Batch GitHub API enrichment
"""

import os
import sys
import happybase
import logging
from pathlib import Path
from urllib.parse import quote
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, lag, coalesce, lit, desc
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
load_dotenv(BASE_DIR / ".env")

# ── Configuration ─────────────────────────────────────────────
HBASE_HOST  = os.getenv("HBASE_HOST",  "hadoop-master")
HBASE_PORT  = int(os.getenv("HBASE_PORT", "9090"))
HDFS_HOST   = os.getenv("HDFS_HOST",   "hadoop-master:9000")
HDFS_INPUT  = f"hdfs://{HDFS_HOST}/user/root/gharchive/*.json.gz"

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("batch_job")


def resolve_input_path() -> str:
    """Use HDFS as the single source of truth for GH Archive data."""
    log.info("Using HDFS input only for batch processing")
    return HDFS_INPUT


def get_processed_weeks(connection) -> set:
    """Get weeks already processed from HBase weekly_metrics."""
    processed = set()
    try:
        table = connection.table("weekly_metrics")
        for row_key, data in table.scan():
            row_str = row_key.decode("utf-8")
            week = row_str.split("#")[0]
            processed.add(week)
        log.info(f"Found {len(processed)} already processed weeks")
    except Exception:
        log.info("No weekly_metrics table found, starting fresh")
    return processed


def compute_actual_velocity(df_weekly, connection):
    """
    Compute actual velocity: stars this week - stars last week.
    Also stores current week's data for next run's comparison.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, lead, coalesce, lit

    window_spec = Window.partitionBy("repo_name").orderBy("week")

    with_velocity = df_weekly.withColumn(
        "prev_stars",
        lag("star_count", 1).over(window_spec)
    ).withColumn(
        "velocity",
        coalesce(col("star_count") - col("prev_stars"), col("star_count"))
    ).withColumn(
        "prev_forks",
        lag("fork_count", 1).over(window_spec)
    ).withColumn(
        "fork_velocity",
        coalesce(col("fork_count") - col("prev_forks"), col("fork_count"))
    ).drop("prev_stars", "prev_forks")

    return with_velocity


def get_hbase_connection():
    return happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=10000
    )


def build_repo_key(repo_name: str) -> bytes:
    """Build a deterministic, collision-safe repo key used across HBase tables."""
    return quote(repo_name, safe="").encode("utf-8")

def batch_enrich_with_language(repo_names: list) -> dict:
    """
    Call GitHub API to get language for each repo using batch GraphQL query.
    Limits to 50 repos per request to avoid rate limits.
    """
    import requests

    if not repo_names:
        return {}

    language_map = {repo: "Unknown" for repo in repo_names}
    headers = {
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
        "Accept": "application/vnd.github+json"
    }

    log.info(f"Fetching language for {len(repo_names)} repos via GraphQL...")

    # Process in batches of 50
    batch_size = 50
    for i in range(0, len(repo_names), batch_size):
        batch = repo_names[i:i + batch_size]

        # Build GraphQL query for this batch
        queries = "\n".join([f'{repo.split("/")[1]}: repository(owner: "{repo.split("/")[0]}", name: "{repo.split("/")[1]}") {{ language {{ name }} }}' for repo in batch])

        query = f"""{{
 {queries}
}}"""

        try:
            r = requests.post(
                "https://api.github.com/graphql",
                json={"query": query},
                headers=headers,
                timeout=30
            )

            if r.status_code == 200:
                data = r.json().get("data", {})
                for repo in batch:
                    try:
                        key = repo.split("/")[1]
                        lang_data = data.get(key, {})
                        lang = lang_data.get("language", {})
                        language_map[repo] = lang.get("name", "Unknown") if lang else "Unknown"
                    except Exception:
                        language_map[repo] = "Unknown"
            else:
                log.warning(f"GraphQL request failed: {r.status_code}")

        except Exception as e:
            log.warning(f"Error fetching languages: {e}")
            # Fall back to individual requests
            for repo in batch:
                try:
                    r = requests.get(
                        f"https://api.github.com/repos/{repo}",
                        headers=headers,
                        timeout=5
                    )
                    if r.status_code == 200:
                        language_map[repo] = r.json().get("language") or "Unknown"
                except Exception:
                    language_map[repo] = "Unknown"

    log.info("Language enrichment complete")
    return language_map

def write_weekly_metrics_bulk(df_weekly, language_map=None):
    """
    Write weekly aggregated metrics to HBase using batch writes.
    Also stores the latest week data for velocity calculation in next run.

    Row key: week#repo_name
    Column families: repo (name, language) / stats (stars, forks, velocity)
    """
    rows = df_weekly.collect()
    log.info(f"Bulk writing {len(rows)} rows to weekly_metrics...")

    connection = get_hbase_connection()
    table = connection.table("weekly_metrics")

    # Prepare batch puts
    batch_puts = []
    last_week_puts = []

    # Get max week from this run
    max_week = None
    for row in rows:
        week = row["week"]
        if week:
            if max_week is None or week > max_week:
                max_week = week

    for row in rows:
        repo_name  = row["repo_name"]  or "unknown"
        week       = row["week"]       or "unknown"
        stars      = str(row["star_count"]  or 0)
        forks      = str(row["fork_count"]  or 0)
        velocity   = str(row["velocity"]    or 0)
        repo_key   = build_repo_key(repo_name)

        row_key = f"{week}#{repo_name}".encode("utf-8")
        language = language_map.get(repo_name, "Unknown") if language_map else "Unknown"

        # Main weekly metrics
        batch_puts.append((row_key, {
            b"repo:name":       repo_name.encode("utf-8"),
            b"repo:language":   language.encode("utf-8"),
            b"repo:key":        repo_key,
            b"stats:stars":     stars.encode("utf-8"),
            b"stats:forks":     forks.encode("utf-8"),
            b"stats:velocity":  velocity.encode("utf-8"),
            b"stats:week":      week.encode("utf-8"),
        }))

        # Store last week's data for velocity calculation (use max week only)
        if week == max_week:
            last_week_puts.append((f"last_week_data_{repo_name}".encode("utf-8"), {
                b"stats:last_stars": stars.encode("utf-8"),
                b"stats:last_forks": forks.encode("utf-8"),
                b"stats:last_week": week.encode("utf-8"),
            }))

    # Batch write main metrics
    with table.batch() as batch:
        for row_key, data in batch_puts:
            batch.put(row_key, data)

    # Batch write last week reference data
    if last_week_puts:
        last_week_table = connection.table("weekly_metrics")
        with last_week_table.batch() as batch:
            for row_key, data in last_week_puts:
                batch.put(row_key, data)

    connection.close()
    log.info("weekly_metrics bulk write complete")


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

    # ── Get already processed weeks from HBase ────────────────
    connection = get_hbase_connection()
    processed_weeks = get_processed_weeks(connection)
    log.info(f"Skipping already processed weeks: {processed_weeks}")

    # ── Read GH Archive files from HDFS ───────────────────────
    log.info("Reading GH Archive files...")
    try:
        raw = spark.read.option("badRecordsPath", "/tmp/bad_records").json(input_path)
    except Exception as e:
        log.error(f"Failed to read GH Archive input: {e}")
        log.error(f"Checked HDFS path:  {HDFS_INPUT}")
        log.warning("Attempting to read valid files only...")
        try:
            raw = spark.read.option("badRecordsPath", "/tmp/bad_records").option("mode", "PERMISSIVE").json(input_path)
            log.info(f"Recovered: read {raw.count()} valid events after skipping corrupted files")
        except Exception as e2:
            log.error(f"Unable to recover from read failure: {e2}")
            sys.exit(1)

    log.info(f"Total events loaded: {raw.count()}")

    # ── Filter relevant events (WatchEvent and ForkEvent ONLY) ──
    relevant = raw.filter(
        col("type").isin(["WatchEvent", "ForkEvent"])
    )

    # ── Extract week from created_at ───────────────────────────
    from pyspark.sql.functions import date_trunc, to_timestamp

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

    # ── Initial aggregation per repo per week ────────────────────
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
    )

    # ── Skip already processed weeks ─────────────────────────
    if processed_weeks:
        weekly = weekly.filter(~col("week").isin(*processed_weeks))

    # Check if there's data to process
    if weekly.count() == 0:
        log.info("No new weeks to process. Exiting.")
        spark.stop()
        return

    # ── Compute actual velocity (compare to previous week) ───
    weekly_with_velocity = compute_actual_velocity(weekly, connection)

    # Sort for display and processing
    weekly_with_velocity = weekly_with_velocity.orderBy(desc("velocity"))

    log.info("Weekly aggregation complete")
    weekly_with_velocity.show(10, truncate=False)

    connection.close()

    # ── Get top repos for language enrichment ───────────────
    top_repos = weekly_with_velocity.orderBy(desc("velocity")).limit(200)
    repo_names = [row["repo_name"] for row in top_repos.collect()]

    # ── Enrich with language (batch GraphQL) ─────────────────
    language_map = batch_enrich_with_language(repo_names)

    # ── Write to HBase (bulk write) ──────────────────────
    write_weekly_metrics_bulk(weekly_with_velocity, language_map)

    log.info("Batch job complete")
    spark.stop()


if __name__ == "__main__":
    main()