from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator


gharchive_hdfs_path = os.getenv("GHARCHIVE_HDFS_PATH", "/user/root/gharchive")
retention_days = int(os.getenv("GHARCHIVE_RETENTION_DAYS", "30"))

with DAG(
    dag_id="hdfs_daily_batch",
    description="Run GHArchive daily batch from HDFS into HBase",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",  # Every day at 02:00
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "opentrend",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["opentrend", "hdfs", "spark"],
) as dag:

    def build_ingest_command(hdfs_path: str) -> str:
        return (
            "docker exec hadoop-master bash -lc '\n"
            "set -euo pipefail\n"
            f"hdfs_path={hdfs_path}\n"
            "verify_hdfs_gzip() {\n"
            "  local file_path=\"$1\"\n"
            "  hdfs dfs -cat \"$file_path\" 2>/dev/null | gzip -t 2>/dev/null\n"
            "}\n"
            "hdfs dfs -mkdir -p \"$hdfs_path\"\n"
            "workdir=/tmp/gharchive-batch\n"
            "mkdir -p \"$workdir\"\n"
            "cd \"$workdir\"\n"
            "valid_count=0\n"
            "skip_count=0\n"
            "download_count=0\n"
            "for offset in 1; do\n"
            "  day=$(date -u -d \"-$offset day\" +%Y-%m-%d)\n"
            "  base_name=\"$day.json.gz\"\n"
            "  if hdfs dfs -test -e \"$hdfs_path/$base_name\"; then\n"
            "    if verify_hdfs_gzip \"$hdfs_path/$base_name\"; then\n"
            "      echo \"Skipping $base_name - already exists and is valid in HDFS\"\n"
            "      skip_count=$((skip_count + 1))\n"
            "      continue\n"
            "    fi\n"
            "    echo \"Found corrupted HDFS file $base_name - deleting and re-downloading\" >&2\n"
            "    hdfs dfs -rm -f \"$hdfs_path/$base_name\"\n"
            "  fi\n"
            "  if wget -q -O \"$base_name\" \"https://data.gharchive.org/$base_name\"; then\n"
            "    if gzip -t \"$base_name\" 2>/dev/null; then\n"
            "      hdfs dfs -put -f \"$base_name\" \"$hdfs_path/\"\n"
            "      rm -f \"$base_name\"\n"
            "      valid_count=$((valid_count + 1))\n"
            "      download_count=$((download_count + 1))\n"
            "      echo \"Downloaded and verified $base_name\"\n"
            "      continue\n"
            "    else\n"
            "      echo \"Invalid gzip for $base_name - trying hourly files\" >&2\n"
            "      rm -f \"$base_name\"\n"
            "    fi\n"
            "  else\n"
            "    rm -f \"$base_name\"\n"
            "  fi\n"
            "  got_any=0\n"
            "  for hour in $(seq 0 23); do\n"
            "    hourly=\"$day-$hour.json.gz\"\n"
            "    if hdfs dfs -test -e \"$hdfs_path/$hourly\"; then\n"
            "      if verify_hdfs_gzip \"$hdfs_path/$hourly\"; then\n"
            "        echo \"Skipping $hourly - already exists and is valid in HDFS\"\n"
            "        skip_count=$((skip_count + 1))\n"
            "        continue\n"
            "      fi\n"
            "      echo \"Found corrupted HDFS file $hourly - deleting and re-downloading\" >&2\n"
            "      hdfs dfs -rm -f \"$hdfs_path/$hourly\"\n"
            "    fi\n"
            "    if wget -q -O \"$hourly\" \"https://data.gharchive.org/$hourly\"; then\n"
            "      if gzip -t \"$hourly\" 2>/dev/null; then\n"
            "        hdfs dfs -put -f \"$hourly\" \"$hdfs_path/\"\n"
            "        rm -f \"$hourly\"\n"
            "        valid_count=$((valid_count + 1))\n"
            "        download_count=$((download_count + 1))\n"
            "        got_any=1\n"
            "        echo \"Downloaded and verified $hourly\"\n"
            "      else\n"
            "        echo \"Invalid gzip for $hourly - skipping\" >&2\n"
            "        rm -f \"$hourly\"\n"
            "      fi\n"
            "    else\n"
            "      rm -f \"$hourly\"\n"
            "    fi\n"
            "  done\n"
            "  if [ \"$valid_count\" -eq 0 ] && [ \"$got_any\" -eq 0 ]; then\n"
            "    echo \"No GHArchive file found for $day\" >&2\n"
            "  fi\n"
            "done\n"
            "echo \"Summary: downloaded=$download_count skipped=$skip_count verified=$valid_count\"\n"
            "hdfs dfs -count \"$hdfs_path\"\n"
            "'\n"
        )

    def build_cleanup_command(hdfs_path: str, retention_days: int) -> str:
        return (
            "docker exec hadoop-master bash -lc '\n"
            "set -euo pipefail\n"
            f"hdfs_path={hdfs_path}\n"
            f"retention_days={retention_days}\n"
            "cutoff_date=$(date -u -d \"-$retention_days days\" +%Y-%m-%d)\n"
            "echo \"Cleaning up files older than $cutoff_date\"\n"
            "deleted=0\n"
            "hdfs dfs -ls \"$hdfs_path\" | grep -E '\\.json\\.gz$' | while read -r perms repl owner group size date time path; do\n"
            "  file_date=$(echo \"$path\" | sed -n 's|.*/\\([0-9]\\{4\\}-[0-9]\\{2\\}-[0-9]\\{2\\}\\).*|\\1|p')\n"
            "  if [[ -z \"$file_date\" ]]; then\n"
            "    echo \"Skipping $path - could not extract date from filename\"\n"
            "    continue\n"
            "  fi\n"
            "  if [[ \"$file_date\" < \"$cutoff_date\" ]]; then\n"
            "    hdfs dfs -rm -f \"$path\"\n"
            "    deleted=$((deleted + 1))\n"
            "    echo \"Deleted $path (dated $file_date)\"\n"
            "  fi\n"
            "done\n"
            "echo \"Deleted $deleted old files\"\n"
            "'\n"
        )

    check_runtime_dependencies = BashOperator(
        task_id="check_runtime_dependencies",
        bash_command="command -v docker >/dev/null 2>&1",
    )

    ingest_previous_day_to_hdfs = BashOperator(
        task_id="ingest_previous_day_to_hdfs",
        bash_command=build_ingest_command(gharchive_hdfs_path),
    )

    check_hdfs_input = BashOperator(
        task_id="check_hdfs_input",
        bash_command=(
            f"docker exec hadoop-master hdfs dfs -test -e {gharchive_hdfs_path} && "
            f"docker exec hadoop-master hdfs dfs -count {gharchive_hdfs_path}"
        ),
    )

    run_spark_batch = BashOperator(
        task_id="run_spark_batch",
        bash_command=(
            "docker exec opentrend-spark "
            "spark-submit --master local[*] /app/batch_job.py"
        ),
    )

    cleanup_old_hdfs_files = BashOperator(
        task_id="cleanup_old_hdfs_files",
        bash_command=build_cleanup_command(gharchive_hdfs_path, retention_days),
    )

    (
        check_runtime_dependencies
        >> ingest_previous_day_to_hdfs
        >> check_hdfs_input
        >> run_spark_batch
        >> cleanup_old_hdfs_files
    )