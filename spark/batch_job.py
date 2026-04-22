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
    col, count, sum as spark_sum, lag, coalesce, lit, desc, when
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
load_dotenv(BASE_DIR / ".env")

# ── Configuration ─────────────────────────────────────────────
HBASE_HOST  = os.getenv("HBASE_HOST",  "hadoop-master")
HBASE_PORT  = int(os.getenv("HBASE_PORT", "9090"))
HDFS_HOST   = os.getenv("HDFS_HOST",   "hadoop-master:9000")
GHARCHIVE_HDFS_PATH = os.getenv("GHARCHIVE_HDFS_PATH", "/user/root/gharchive")
DEBUG_DAY = os.getenv("DEBUG_DAY", "")  # For testing single day: "2026-04-13"
if DEBUG_DAY:
    HDFS_INPUT = f"hdfs://{HDFS_HOST}{GHARCHIVE_HDFS_PATH}/{DEBUG_DAY}.json.gz"
    print(f"DEBUG: Processing single day {DEBUG_DAY}")
else:
    HDFS_INPUT  = f"hdfs://{HDFS_HOST}{GHARCHIVE_HDFS_PATH}/*.json.gz"

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


def lookup_repo_languages(repo_names: list) -> dict:
    """
    Prefer repo language data already written by the streaming job in the repos table.
    This avoids depending on GitHub API enrichment when streaming has already resolved
    the language label.
    """
    if not repo_names:
        return {}

    connection = get_hbase_connection()
    repos_table = connection.table("repos")
    result = {}

    try:
        for repo_name in repo_names:
            repo_key = build_repo_key(repo_name)
            row = repos_table.row(repo_key)
            language = (
                row.get(b"info:language", b"")
                .decode("utf-8", errors="ignore")
                .strip()
            )
            if language:
                result[repo_name] = language
    finally:
        connection.close()

    return result

def batch_enrich_repos(repo_names: list) -> dict:
    """
    Enrich repos with language, stargazers_count, and forks_count from GitHub API.
    Uses batch GraphQL query, limits to 50 repos per request to avoid rate limits.
    Returns dict with repo_name -> {language, stargazers_count, forks_count}
    """
    import requests

    if not repo_names:
        return {}

    enrichment_map = {
        repo: {"language": "Unknown", "stargazers_count": 0, "forks_count": 0}
        for repo in repo_names
    }

    # First hydrate from the streaming pipeline's repos table.
    hbase_languages = lookup_repo_languages(repo_names)
    for repo, language in hbase_languages.items():
        enrichment_map[repo]["language"] = language

    repos_to_query = [
        repo for repo in repo_names
        if enrichment_map.get(repo, {}).get("language") in ("", "Unknown", None)
    ]

    if not repos_to_query:
        log.info("Repo languages already present in HBase; skipping GraphQL enrichment")
        return enrichment_map

    headers = {
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
        "Accept": "application/vnd.github+json"
    }

    log.info(f"Enriching {len(repos_to_query)} repos with language, stars, and forks via GraphQL...")

    # Process in batches of 50
    batch_size = 50
    for i in range(0, len(repos_to_query), batch_size):
        batch = repos_to_query[i:i + batch_size]

        # Build GraphQL query for this batch with safe aliases (r0, r1, ...).
        # Repo names may contain characters that are invalid as GraphQL aliases.
        alias_to_repo = {}
        queries = []
        for idx, repo in enumerate(batch):
            try:
                owner, name = repo.split("/", 1)
            except ValueError:
                continue
            alias = f"r{idx}"
            alias_to_repo[alias] = repo
            queries.append(
                f'{alias}: repository(owner: "{owner}", name: "{name}") {{ language {{ name }} stargazersCount forkCount }}'
            )

        if not queries:
            continue

        query = "{\n" + "\n".join(queries) + "\n}"

        unresolved = list(batch)

        try:
            r = requests.post(
                "https://api.github.com/graphql",
                json={"query": query},
                headers=headers,
                timeout=30
            )

            if r.status_code == 200:
                payload = r.json()
                data = payload.get("data", {}) or {}
                errors = payload.get("errors", []) or []

                if errors:
                    log.warning(f"GraphQL returned {len(errors)} errors in current batch")

                resolved = set()
                for alias, repo in alias_to_repo.items():
                    try:
                        repo_data = data.get(alias)
                        if not repo_data:
                            continue
                        lang = repo_data.get("language", {})
                        language = lang.get("name", "Unknown") if lang else "Unknown"
                        stargazers = repo_data.get("stargazersCount", 0) or 0
                        forks = repo_data.get("forkCount", 0) or 0
                        enrichment_map[repo] = {
                            "language": language,
                            "stargazers_count": stargazers,
                            "forks_count": forks,
                        }
                        resolved.add(repo)
                    except Exception:
                        pass

                unresolved = [repo for repo in batch if repo not in resolved]
            else:
                log.warning(f"GraphQL request failed: {r.status_code}")
                unresolved = list(batch)

        except Exception as e:
            log.warning(f"Error enriching repos: {e}")
            unresolved = list(batch)

        # Fall back to individual REST requests for unresolved repos.
        for repo in unresolved:
            if enrichment_map.get(repo, {}).get("language") not in ("", "Unknown", None):
                continue
            try:
                r = requests.get(
                    f"https://api.github.com/repos/{repo}",
                    headers=headers,
                    timeout=5
                )
                if r.status_code == 200:
                    repo_json = r.json()
                    enrichment_map[repo] = {
                        "language": repo_json.get("language") or "Unknown",
                        "stargazers_count": repo_json.get("stargazers_count") or 0,
                        "forks_count": repo_json.get("forks_count") or 0,
                    }
            except Exception:
                enrichment_map[repo] = {"language": "Unknown", "stargazers_count": 0, "forks_count": 0}

    log.info("Repo enrichment complete")
    return enrichment_map


def backfill_unknown_languages(limit_rows: int = 5000):
    """
    Backfill Unknown languages for existing weekly rows when there is no new week.
    This keeps Streamlit language panels usable even on repeated runs.
    """
    connection = get_hbase_connection()
    table = connection.table("weekly_metrics")
    repos_table = connection.table("repos")
    import requests

    to_update = []
    repos = set()

    for row_key, cells in table.scan(limit=limit_rows):
        row_str = row_key.decode("utf-8", errors="ignore")
        if "#" not in row_str:
            continue

        repo_name = cells.get(b"repo:name", b"").decode("utf-8", errors="ignore")
        if not repo_name:
            repo_name = row_str.split("#", 1)[1]

        lang = cells.get(b"repo:language", b"").decode("utf-8", errors="ignore").strip()
        if lang and lang != "Unknown":
            continue

        to_update.append((row_key, repo_name))
        repos.add(repo_name)

    if not to_update:
        log.info("No Unknown languages to backfill")
        connection.close()
        return

    log.info(f"Backfilling language for {len(to_update)} rows ({len(repos)} repos)")

    # Language backfill does not need star/fork enrichment. Prefer HBase first,
    # then fall back to the REST repo endpoint for the few repos still missing.
    hbase_languages = lookup_repo_languages(list(repos))
    api_languages = {}
    headers = {
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN')}",
        "Accept": "application/vnd.github+json"
    }

    missing_repos = [repo for repo in repos if repo not in hbase_languages]
    for repo_name in missing_repos:
        try:
            r = requests.get(
                f"https://api.github.com/repos/{repo_name}",
                headers=headers,
                timeout=10
            )
            if r.status_code == 200:
                repo_json = r.json()
                language = (repo_json.get("language") or "").strip()
                if language:
                    api_languages[repo_name] = language
        except Exception:
            continue

    updates = 0
    with table.batch() as batch:
        for row_key, repo_name in to_update:
            language = hbase_languages.get(repo_name, "")
            if not language:
                repo_row = repos_table.row(build_repo_key(repo_name))
                language = (
                    repo_row.get(b"info:language", b"")
                    .decode("utf-8", errors="ignore")
                    .strip()
                )
            if not language:
                language = api_languages.get(repo_name, "")

            if not language or language == "Unknown":
                continue

            payload = {b"repo:language": language.encode("utf-8")}

            batch.put(row_key, payload)
            updates += 1

    connection.close()
    log.info(f"Language backfill updated {updates} rows")

def write_weekly_metrics_bulk(df_weekly, enrichment_map=None):
    """
    Write weekly aggregated metrics to HBase using batch writes.
    Also stores the latest week data for velocity calculation in next run.
    Enriches with real-time GitHub API star/fork counts when available.

    Row key: week#repo_name
    Column families: repo (name, language) / stats (stars, forks, velocity, api_stars, api_forks)
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
        
        # Get enrichment data (language, real-time stars/forks from GitHub API)
        enrichment = enrichment_map.get(repo_name, {}) if enrichment_map else {}
        language = enrichment.get("language", "Unknown")
        api_stars = str(enrichment.get("stargazers_count", 0))
        api_forks = str(enrichment.get("forks_count", 0))

        # Main weekly metrics
        batch_puts.append((row_key, {
            b"repo:name":       repo_name.encode("utf-8"),
            b"repo:language":   language.encode("utf-8"),
            b"repo:key":        repo_key,
            b"stats:stars":     stars.encode("utf-8"),
            b"stats:forks":     forks.encode("utf-8"),
            b"stats:velocity":  velocity.encode("utf-8"),
            b"stats:api_stars": api_stars.encode("utf-8"),
            b"stats:api_forks": api_forks.encode("utf-8"),
            b"stats:week":      week.encode("utf-8"),
        }))

        # Store last week's data for velocity calculation (use max week only)
        if week == max_week:
            last_week_puts.append((f"last_week_data_{repo_name}".encode("utf-8"), {
                b"stats:last_stars": stars.encode("utf-8"),
                b"stats:last_forks": forks.encode("utf-8"),
                b"stats:last_api_stars": api_stars.encode("utf-8"),
                b"stats:last_api_forks": api_forks.encode("utf-8"),
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
        log.info("No new weeks to process. Running language backfill for existing rows...")
        backfill_unknown_languages(limit_rows=10000)
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

    # ── Get top repos for enrichment ───────────────
    top_repos = weekly_with_velocity.orderBy(desc("velocity")).limit(200)
    repo_names = [row["repo_name"] for row in top_repos.collect()]

    # ── Enrich with language, stars, and forks (batch GraphQL) ─────────────────
    enrichment_map = batch_enrich_repos(repo_names)

    # ── Write to HBase (bulk write) ──────────────────────
    write_weekly_metrics_bulk(weekly_with_velocity, enrichment_map)

    log.info("Batch job complete")
    spark.stop()


if __name__ == "__main__":
    main()