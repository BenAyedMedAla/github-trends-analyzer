"""
opentrend — producer.py
------------------------
Polls the GitHub Events API every 60 seconds and publishes
WatchEvents (stars) and ForkEvents to the Kafka topic
'github-events'.

Same role as SimpleProducer.java from TP3, but in Python.
"""

import os
import json
import time
import logging
import requests
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Load environment variables from .env
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
load_dotenv(BASE_DIR / ".env")

# ── Configuration (from environment variables) ────────────────
KAFKA_HOST  = os.getenv("KAFKA_HOST", "hadoop-master:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github-events")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
GITHUB_API_URL = "https://api.github.com/events?per_page=100"

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("producer")

# ── Events we care about ──────────────────────────────────────
RELEVANT_TYPES = {"WatchEvent", "ForkEvent"}

# ── Seen event IDs to avoid duplicates ───────────────────────
# (Kafka guarantees at-least-once, so we deduplicate here)
seen_ids = set()


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with retry logic."""
    retries = 10
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_HOST,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",          # wait for broker acknowledgement
                retries=3,
            )
            log.info(f"Connected to Kafka at {KAFKA_HOST}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka not ready, retrying ({attempt+1}/{retries})...")
            time.sleep(10)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def build_headers() -> dict:
    """Build GitHub API request headers."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


def resolve_fork(event: dict) -> str:
    """
    Return the canonical repo name.
    If the repo is a fork, return the parent repo name instead.
    This prevents the trending list from being polluted with
    fork copies of the same project (critical data quality step).
    """
    repo_name = event.get("repo", {}).get("name", "")
    payload   = event.get("payload", {})

    # ForkEvent payload contains the forkee which has parent info
    forkee = payload.get("forkee", {})
    if forkee.get("fork") and forkee.get("parent"):
        parent_name = forkee["parent"].get("full_name", repo_name)
        log.debug(f"Fork resolved: {repo_name} -> {parent_name}")
        return parent_name

    return repo_name


def fetch_events() -> list:
    """Fetch the latest public GitHub events."""
    try:
        response = requests.get(
            GITHUB_API_URL,
            headers=build_headers(),
            timeout=10
        )

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 403:
            log.warning("GitHub API rate limit reached. Add a token to increase limit.")
            return []
        elif response.status_code == 304:
            log.info("No new events since last poll.")
            return []
        else:
            log.error(f"GitHub API error: {response.status_code}")
            return []

    except requests.exceptions.RequestException as e:
        log.error(f"Network error fetching events: {e}")
        return []


def process_and_publish(producer: KafkaProducer):
    """
    Fetch GitHub events, filter relevant ones,
    and publish each one to Kafka.
    """
    events = fetch_events()

    if not events:
        return

    published = 0
    skipped   = 0

    # DEBUG — print all event types received
    all_types = [e.get("type") for e in events]
    log.info(f"Event types received: {set(all_types)}")
    log.info(f"Total events from API: {len(events)}")

    for event in events:
        event_id   = event.get("id")
        event_type = event.get("type")

        # Skip irrelevant event types
        if event_type not in RELEVANT_TYPES:
            continue

        # Skip already-seen events (deduplication)
        if event_id in seen_ids:
            skipped += 1
            continue

        seen_ids.add(event_id)

        # Resolve fork to canonical parent repo
        canonical_repo = resolve_fork(event)

        # Build the message we send to Kafka
        message = {
            "event_id":   event_id,
            "event_type": event_type,
            "repo_name":  canonical_repo,
            "actor":      event.get("actor", {}).get("login", "unknown"),
            "created_at": event.get("created_at", ""),
        }

        # Publish to Kafka (same as producer.send() in TP3)
        producer.send(KAFKA_TOPIC, value=message)
        published += 1

    # Flush ensures all messages are sent before returning
    producer.flush()

    log.info(
        f"Poll complete — published: {published}, "
        f"skipped (duplicate): {skipped}, "
        f"total seen: {len(seen_ids)}"
    )

    # Keep seen_ids from growing forever
    # (keep only the last 5000 event IDs in memory)
    if len(seen_ids) > 5000:
        oldest = list(seen_ids)[:1000]
        for eid in oldest:
            seen_ids.discard(eid)


def main():
    log.info("opentrend producer starting...")
    log.info(f"  Kafka broker : {KAFKA_HOST}")
    log.info(f"  Kafka topic  : {KAFKA_TOPIC}")
    log.info(f"  Poll interval: {POLL_INTERVAL}s")
    log.info(f"  GitHub token : {'set' if GITHUB_TOKEN else 'NOT SET (60 req/hr limit)'}")

    producer = create_producer()

    log.info("Starting poll loop...")

    while True:
        try:
            process_and_publish(producer)
        except Exception as e:
            log.error(f"Unexpected error during poll: {e}")

        log.info(f"Sleeping {POLL_INTERVAL} seconds...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()