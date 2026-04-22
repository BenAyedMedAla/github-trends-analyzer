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

Enhancements over v1:
  - foreachPartition for HBase writes (no driver OOM)
  - DataFrame cached to avoid double Spark DAG evaluation
  - Parallel REST fallback with ThreadPoolExecutor (10 workers)
  - Dedicated batch_metadata table replaces full weekly_metrics scan
  - stats:week stored as clean YYYY-MM-DD (no Streamlit [:10] hack)
  - stats:repo_count written for correct "Rising Languages" panel
  - stats:fork_velocity written for correct "Trending This Week" score
  - HBase repos table queried for ALL repos before GraphQL (not just top 200)
  - GraphQL enrichment limited to repos still missing after HBase lookup
"""

import os
import sys
import happybase
import logging
from pathlib import Path
from urllib.parse import quote
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, when, desc,
    date_trunc, to_timestamp, coalesce, lit
)
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
load_dotenv(BASE_DIR / ".env")

# ── Configuration ─────────────────────────────────────────────
HBASE_HOST  = os.getenv("HBASE_HOST",  "hadoop-master")
HBASE_PORT  = int(os.getenv("HBASE_PORT", "9090"))
HDFS_HOST   = os.getenv("HDFS_HOST",   "hadoop-master:9000")
GHARCHIVE_HDFS_PATH = os.getenv("GHARCHIVE_HDFS_PATH", "/user/root/gharchive")
DEBUG_DAY   = os.getenv("DEBUG_DAY", "")  # e.g. "2026-04-13"

if DEBUG_DAY:
    HDFS_INPUT = f"hdfs://{HDFS_HOST}{GHARCHIVE_HDFS_PATH}/{DEBUG_DAY}.json.gz"
    print(f"DEBUG: Processing single day {DEBUG_DAY}")
else:
    HDFS_INPUT = f"hdfs://{HDFS_HOST}{GHARCHIVE_HDFS_PATH}/*.json.gz"

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("batch_job")

GHARCHIVE_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField(
        "repo",
        StructType([
            StructField("name", StringType(), True),
        ]),
        True,
    ),
    StructField("created_at", StringType(), True),
])


# ─────────────────────────────────────────────────────────────
# HBase helpers
# ─────────────────────────────────────────────────────────────

def get_hbase_connection() -> happybase.Connection:
    return happybase.Connection(
        host=HBASE_HOST,
        port=HBASE_PORT,
        timeout=10_000,
    )


def build_repo_key(repo_name) -> bytes:
    """Deterministic, collision-safe repo key used across HBase tables."""
    if repo_name is None:
        return b""

    if isinstance(repo_name, bytes):
        text = repo_name.decode("utf-8", errors="ignore").strip()
    else:
        text = str(repo_name).strip()

    if not text:
        return b""

    return quote(text, safe="").encode("utf-8")


# ─────────────────────────────────────────────────────────────
# Processed-week tracking  (replaces full weekly_metrics scan)
# ─────────────────────────────────────────────────────────────
# We use a single HBase table  batch_metadata  with row key
#   "processed_weeks"  and one qualifier per week:
#   meta:YYYY-MM-DD  →  "1"
# This makes get/set O(1) instead of O(all rows in weekly_metrics).

METADATA_TABLE   = "batch_metadata"
METADATA_ROW_KEY = b"processed_weeks"
METADATA_CF      = b"meta"


def ensure_metadata_table(connection: happybase.Connection) -> None:
    """Create batch_metadata table if it does not exist."""
    tables = [t.decode() for t in connection.tables()]
    if METADATA_TABLE not in tables:
        connection.create_table(
            METADATA_TABLE,
            {METADATA_CF.decode(): dict(max_versions=1)},
        )
        log.info("Created HBase table: %s", METADATA_TABLE)


def get_processed_weeks(connection: happybase.Connection) -> set:
    """
    Return the set of already-processed week strings (YYYY-MM-DD).
    O(1) HBase get instead of a full weekly_metrics scan.
    """
    ensure_metadata_table(connection)
    table = connection.table(METADATA_TABLE)
    row = table.row(METADATA_ROW_KEY)
    processed = {
        k.decode("utf-8").replace(f"{METADATA_CF.decode()}:", "")
        for k in row.keys()
    }
    log.info("Already processed weeks: %s", processed)
    return processed


def mark_weeks_processed(
    connection: happybase.Connection,
    weeks: list,
) -> None:
    """Record newly processed weeks in batch_metadata."""
    if not weeks:
        return
    table = connection.table(METADATA_TABLE)
    table.put(
        METADATA_ROW_KEY,
        {f"{METADATA_CF.decode()}:{w}".encode(): b"1" for w in weeks},
    )
    log.info("Marked weeks as processed: %s", weeks)


# ─────────────────────────────────────────────────────────────
# Velocity computation
# ─────────────────────────────────────────────────────────────


def _compute_velocity(df_weekly):
    """
    Clean velocity computation using explicit imports.
    Replaces compute_actual_velocity above.
    """
    from pyspark.sql.functions import lag as spark_lag

    window_spec = Window.partitionBy("repo_name").orderBy("week")

    return (
        df_weekly
        .withColumn("prev_stars",  spark_lag("star_count",  1).over(window_spec))
        .withColumn("prev_forks",  spark_lag("fork_count",  1).over(window_spec))
        .withColumn("velocity",    coalesce(col("star_count")  - col("prev_stars"),  col("star_count")))
        .withColumn("fork_velocity", coalesce(col("fork_count") - col("prev_forks"), col("fork_count")))
        .drop("prev_stars", "prev_forks")
    )


# ─────────────────────────────────────────────────────────────
# Language resolution — HBase-first, then GitHub API
# ─────────────────────────────────────────────────────────────

def lookup_repo_languages_bulk(repo_names: list) -> dict:
    """
    Batch-fetch language labels from the repos HBase table
    (written by the streaming job).  Returns {repo_name: language}.
    Single connection, one row.get() per repo.
    """
    if not repo_names:
        return {}

    result = {}
    connection = get_hbase_connection()
    try:
        repos_table = connection.table("repos")
        for repo_name in repo_names:
            repo_key = build_repo_key(repo_name)
            if not repo_key:
                continue
            row = repos_table.row(repo_key)
            lang = row.get(b"info:language", b"").decode("utf-8", errors="ignore").strip()
            if lang and lang != "Unknown":
                result[repo_name] = lang
    finally:
        connection.close()

    log.info(
        "HBase repos table resolved %d / %d language labels",
        len(result), len(repo_names),
    )
    return result


def _fetch_repo_rest(repo_name: str, headers: dict) -> dict:
    """Single REST call for one repo.  Used as a thread-pool task."""
    import requests
    try:
        r = requests.get(
            f"https://api.github.com/repos/{repo_name}",
            headers=headers,
            timeout=8,
        )
        if r.status_code == 200:
            j = r.json()
            return {
                "language":        (j.get("language") or "Unknown"),
                "stargazers_count": j.get("stargazers_count") or 0,
                "forks_count":      j.get("forks_count") or 0,
            }
    except Exception:
        pass
    return {"language": "Unknown", "stargazers_count": 0, "forks_count": 0}


def batch_enrich_repos(repo_names: list) -> dict:
    """
    Enrich repos with language, stargazers_count, and forks_count.

    Strategy (in order):
      1. HBase repos table  (populated by streaming job) — free & fast
      2. GitHub GraphQL batch (50 at a time)            — for missing ones
      3. Parallel REST fallback (ThreadPoolExecutor)    — for GraphQL errors

    Returns {repo_name: {language, stargazers_count, forks_count}}
    """
    import requests

    if not repo_names:
        return {}

    enrichment_map: dict = {
        repo: {"language": "Unknown", "stargazers_count": 0, "forks_count": 0}
        for repo in repo_names
    }

    # ── Step 1: HBase lookup for ALL repos ───────────────────
    hbase_languages = lookup_repo_languages_bulk(repo_names)
    for repo, lang in hbase_languages.items():
        enrichment_map[repo]["language"] = lang

    repos_needing_api = [
        r for r in repo_names
        if enrichment_map[r]["language"] in ("Unknown", "", None)
    ]

    if not repos_needing_api:
        log.info("All language labels resolved from HBase — skipping GitHub API")
        return enrichment_map

    log.info(
        "%d repos still need API enrichment after HBase lookup",
        len(repos_needing_api),
    )

    headers = {
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN', '')}",
        "Accept": "application/vnd.github+json",
    }

    # ── Step 2: GraphQL batch (50 per request) ────────────────
    unresolved_after_graphql: list = []
    batch_size = 50

    for i in range(0, len(repos_needing_api), batch_size):
        batch = repos_needing_api[i : i + batch_size]
        alias_to_repo: dict = {}
        queries: list = []

        for idx, repo in enumerate(batch):
            try:
                owner, name = repo.split("/", 1)
            except ValueError:
                unresolved_after_graphql.append(repo)
                continue
            alias = f"r{idx}"
            alias_to_repo[alias] = repo
            queries.append(
                f'{alias}: repository(owner: "{owner}", name: "{name}") '
                f'{{ language {{ name }} stargazersCount forkCount }}'
            )

        if not queries:
            continue

        query = "{\n" + "\n".join(queries) + "\n}"

        try:
            resp = requests.post(
                "https://api.github.com/graphql",
                json={"query": query},
                headers=headers,
                timeout=30,
            )
            if resp.status_code != 200:
                log.warning("GraphQL HTTP %d — queuing batch for REST fallback", resp.status_code)
                unresolved_after_graphql.extend(batch)
                continue

            payload = resp.json()
            data    = payload.get("data") or {}
            errors  = payload.get("errors") or []
            if errors:
                log.warning("GraphQL returned %d errors in batch", len(errors))

            resolved_in_batch: set = set()
            for alias, repo in alias_to_repo.items():
                repo_data = data.get(alias)
                if not repo_data:
                    continue
                lang_obj = repo_data.get("language") or {}
                enrichment_map[repo] = {
                    "language":         lang_obj.get("name") or "Unknown",
                    "stargazers_count": repo_data.get("stargazersCount") or 0,
                    "forks_count":      repo_data.get("forkCount") or 0,
                }
                resolved_in_batch.add(repo)

            unresolved_after_graphql.extend(
                r for r in batch if r not in resolved_in_batch
            )

        except Exception as exc:
            log.warning("GraphQL request failed: %s", exc)
            unresolved_after_graphql.extend(batch)

    # ── Step 3: Parallel REST fallback ───────────────────────
    still_missing = [
        r for r in unresolved_after_graphql
        if enrichment_map.get(r, {}).get("language") in ("Unknown", "", None)
    ]

    if still_missing:
        log.info("REST fallback for %d repos (ThreadPoolExecutor, 10 workers)", len(still_missing))
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_repo = {
                executor.submit(_fetch_repo_rest, repo, headers): repo
                for repo in still_missing
            }
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    enrichment_map[repo] = future.result()
                except Exception as exc:
                    log.warning("REST fetch failed for %s: %s", repo, exc)

    log.info("Repo enrichment complete for %d repos", len(repo_names))
    return enrichment_map


# ─────────────────────────────────────────────────────────────
# HBase write — foreachPartition (no driver collect / OOM risk)
# ─────────────────────────────────────────────────────────────

def _write_partition_to_hbase(
    rows,
    hbase_host: str,
    hbase_port: int,
    enrichment_broadcast: dict,
    max_week: str,
) -> None:
    """
    Executed on each Spark executor partition.
    Opens its own HBase connection and writes in one batch.
    """
    conn  = happybase.Connection(host=hbase_host, port=hbase_port, timeout=10_000)
    table = conn.table("weekly_metrics")

    with table.batch() as batch:
        for row in rows:
            repo_name    = row["repo_name"]  or "unknown"
            week         = row["week"]       or "unknown"
            star_count   = str(row["star_count"]    or 0)
            fork_count   = str(row["fork_count"]    or 0)
            velocity     = str(row["velocity"]      or 0)
            fork_velocity = str(row["fork_velocity"] or 0)
            repo_count   = str(row["repo_count"]    or 1)

            enrichment   = enrichment_broadcast.get(repo_name, {})
            language     = enrichment.get("language", "Unknown")
            api_stars    = str(enrichment.get("stargazers_count", 0))
            api_forks    = str(enrichment.get("forks_count", 0))

            row_key = f"{week}#{repo_name}".encode("utf-8")

            batch.put(row_key, {
                b"repo:name":          repo_name.encode(),
                b"repo:language":      language.encode(),
                b"repo:key":           build_repo_key(repo_name),
                b"stats:stars":        star_count.encode(),
                b"stats:forks":        fork_count.encode(),
                b"stats:velocity":     velocity.encode(),
                b"stats:fork_velocity": fork_velocity.encode(),
                b"stats:repo_count":   repo_count.encode(),
                b"stats:api_stars":    api_stars.encode(),
                b"stats:api_forks":    api_forks.encode(),
                b"stats:week":         week.encode(),
            })

            # Last-week snapshot (only for the latest week in this run)
            if week == max_week:
                batch.put(
                    f"last_week_data_{repo_name}".encode(),
                    {
                        b"stats:last_stars":        star_count.encode(),
                        b"stats:last_forks":        fork_count.encode(),
                        b"stats:last_api_stars":    api_stars.encode(),
                        b"stats:last_api_forks":    api_forks.encode(),
                        b"stats:last_week":         week.encode(),
                    },
                )

    conn.close()


def write_weekly_metrics_bulk(
    df_weekly,
    enrichment_map: dict,
    hbase_host: str,
    hbase_port: int,
) -> None:
    """
    Write weekly metrics to HBase using foreachPartition.
    Spark executors open their own connections — no driver collect().
    enrichment_map is broadcast via a Python closure (small dict, safe).
    """
    # Determine the max week so executors know which rows get the snapshot.
    max_week_row = (
        df_weekly.select("week")
        .orderBy(desc("week"))
        .limit(1)
        .collect()
    )
    max_week = max_week_row[0]["week"] if max_week_row else ""
    log.info("Max week in this run: %s", max_week)

    # Capture in closure (broadcast-like for small dicts)
    _enrichment   = enrichment_map
    _host         = hbase_host
    _port         = hbase_port
    _max_week     = max_week

    def write_partition(rows):
        _write_partition_to_hbase(rows, _host, _port, _enrichment, _max_week)

    log.info("Writing weekly_metrics via foreachPartition...")
    df_weekly.foreachPartition(write_partition)
    log.info("weekly_metrics write complete")


# ─────────────────────────────────────────────────────────────
# Language backfill (unchanged logic, minor cleanup)
# ─────────────────────────────────────────────────────────────

def backfill_unknown_languages(limit_rows: int = 10_000) -> None:
    """
    Backfill 'Unknown' language rows in weekly_metrics.
    Prefers HBase repos table; falls back to REST API.
    """
    import requests

    connection = get_hbase_connection()
    table      = connection.table("weekly_metrics")
    headers    = {
        "Authorization": f"Bearer {os.getenv('GITHUB_TOKEN', '')}",
        "Accept":        "application/vnd.github+json",
    }

    to_update: list = []
    repos: set      = set()

    for row_key, cells in table.scan(limit=limit_rows):
        row_str   = row_key.decode("utf-8", errors="ignore")
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

    log.info("Backfilling language for %d rows (%d repos)", len(to_update), len(repos))

    # HBase-first
    hbase_languages = lookup_repo_languages_bulk(list(repos))

    # Parallel REST for remainder
    missing = [r for r in repos if r not in hbase_languages]
    api_languages: dict = {}
    if missing:
        log.info("REST backfill for %d repos", len(missing))
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_repo = {
                executor.submit(_fetch_repo_rest, repo, headers): repo
                for repo in missing
            }
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    data = future.result()
                    lang = data.get("language", "")
                    if lang and lang != "Unknown":
                        api_languages[repo] = lang
                except Exception:
                    pass

    updates = 0
    repos_table = connection.table("repos")
    with table.batch() as batch:
        for row_key, repo_name in to_update:
            language = hbase_languages.get(repo_name, "")
            if not language:
                # secondary check directly against repos table
                row = repos_table.row(build_repo_key(repo_name))
                language = (
                    row.get(b"info:language", b"")
                    .decode("utf-8", errors="ignore")
                    .strip()
                )
            if not language:
                language = api_languages.get(repo_name, "")
            if not language or language == "Unknown":
                continue
            batch.put(row_key, {b"repo:language": language.encode("utf-8")})
            updates += 1

    connection.close()
    log.info("Language backfill updated %d rows", updates)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    log.info("opentrend Spark Batch job starting...")
    log.info("  Input path : %s", HDFS_INPUT)
    log.info("  HBase      : %s:%d", HBASE_HOST, HBASE_PORT)

    # ── Spark session ─────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("opentrend-batch")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    log.info("Spark session created")

    # ── Processed weeks (fast O(1) HBase get) ─────────────────
    connection = get_hbase_connection()
    processed_weeks = get_processed_weeks(connection)
    connection.close()

    # ── Read GH Archive from HDFS ─────────────────────────────
    log.info("Reading GH Archive files from HDFS...")
    try:
        raw = (
            spark.read
            .option("badRecordsPath", "/tmp/bad_records")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .schema(GHARCHIVE_SCHEMA)
            .json(HDFS_INPUT)
        )
    except Exception as exc:
        log.error("Failed to read GH Archive input: %s", exc)
        sys.exit(1)

    # ── Filter: WatchEvent + ForkEvent only ───────────────────
    relevant = raw.filter(col("type").isin(["WatchEvent", "ForkEvent"]))

    # ── Extract clean YYYY-MM-DD week column ──────────────────
    events = relevant.select(
        col("type").alias("event_type"),
        col("repo.name").alias("repo_name"),
        col("created_at"),
        date_trunc("week", to_timestamp(col("created_at")))
            .cast("date")
            .cast("string")
            .alias("week"),
    )

    # ── Weekly aggregation ────────────────────────────────────
    weekly = (
        events
        .groupBy("week", "repo_name")
        .agg(
            count(when(col("event_type") == "WatchEvent", 1)).alias("star_count"),
            count(when(col("event_type") == "ForkEvent",  1)).alias("fork_count"),
            countDistinct("repo_name").alias("repo_count"),   # for Rising Languages
            count("*").alias("total_events"),
        )
        .filter(col("repo_name").isNotNull() & col("week").isNotNull())
    )

    # ── Skip already-processed weeks ─────────────────────────
    if processed_weeks:
        weekly = weekly.filter(~col("week").isin(*processed_weeks))

    # ── CACHE before any action to avoid double evaluation ────
    weekly.cache()
    new_row_count = weekly.count()  # single Spark action

    if new_row_count == 0:
        log.info("No new weeks found — running language backfill only.")
        backfill_unknown_languages(limit_rows=10_000)
        weekly.unpersist()
        spark.stop()
        return

    log.info("New rows to process: %d", new_row_count)

    # ── Velocity (window over cached DF) ─────────────────────
    weekly_with_velocity = _compute_velocity(weekly)

    # Sort for readability in logs
    weekly_with_velocity = weekly_with_velocity.orderBy(desc("velocity"))
    weekly_with_velocity.show(10, truncate=False)

    # ── Collect repo names for enrichment ────────────────────
    # Collect ALL unique repos in this run (not just top 200).
    repo_names: list = []
    for row in weekly_with_velocity.select("repo_name").distinct().collect():
        repo = row["repo_name"]
        if repo is None:
            continue
        repo = str(repo).strip()
        if not repo:
            continue
        repo_names.append(repo)

    log.info("Unique repos to enrich: %d", len(repo_names))

    # ── Enrich (HBase → GraphQL → parallel REST) ─────────────
    enrichment_map = batch_enrich_repos(repo_names)

    # ── Write to HBase via foreachPartition ───────────────────
    write_weekly_metrics_bulk(
        weekly_with_velocity,
        enrichment_map,
        hbase_host=HBASE_HOST,
        hbase_port=HBASE_PORT,
    )

    # ── Mark weeks as processed ───────────────────────────────
    new_weeks = [
        row["week"]
        for row in weekly_with_velocity.select("week").distinct().collect()
    ]
    connection = get_hbase_connection()
    mark_weeks_processed(connection, new_weeks)
    connection.close()

    weekly.unpersist()
    log.info("Batch job complete")
    spark.stop()


if __name__ == "__main__":
    main()