"""
opentrend — scheduler.py
--------------------------
Triggers the Spark batch job once a week (every Monday at 02:00).
Runs inside the producer container alongside producer.py.
Replaces Airflow — same logic, zero extra overhead.
"""

import os
import logging
import subprocess
import schedule
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger("scheduler")


def run_batch_job():
    log.info("Triggering weekly batch job...")
    try:
        result = subprocess.run(
            [
                "docker", "exec", "opentrend-spark",
                "spark-submit",
                "--master", "local[*]",
                "/app/batch_job.py"
            ],
            capture_output=True,
            text=True,
            timeout=3600   # 1 hour max
        )
        if result.returncode == 0:
            log.info("Batch job completed successfully")
        else:
            log.error(f"Batch job failed: {result.stderr}")
    except Exception as e:
        log.error(f"Failed to trigger batch job: {e}")


# Schedule every Monday at 02:00
schedule.every().monday.at("02:00").do(run_batch_job)

log.info("Scheduler started — batch job runs every Monday at 02:00")
log.info("To run batch job manually, trigger: docker exec opentrend-spark spark-submit /app/batch_job.py")

while True:
    schedule.run_pending()
    time.sleep(60)