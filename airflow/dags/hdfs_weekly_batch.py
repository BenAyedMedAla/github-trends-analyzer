from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hdfs_weekly_batch",
    description="Run GHArchive weekly batch from HDFS into HBase",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * 1",  # Every Monday 02:00
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
    check_runtime_dependencies = BashOperator(
        task_id="check_runtime_dependencies",
        bash_command="command -v docker >/dev/null 2>&1",
    )

    ingest_last_7_days_to_hdfs = BashOperator(
        task_id="ingest_last_7_days_to_hdfs",
        bash_command=(
            "docker exec hadoop-master bash -lc '\n"
            "set -euo pipefail\n"
            "hdfs dfs -mkdir -p /user/root/gharchive\n"
            "workdir=/tmp/gharchive-batch\n"
            "mkdir -p \"$workdir\"\n"
            "cd \"$workdir\"\n"
            "for offset in 1 2 3 4 5 6 7; do\n"
            "  day=$(date -u -d \"-$offset day\" +%Y-%m-%d)\n"
            "  daily=\"$day.json.gz\"\n"
            "  if wget -q -O \"$daily\" \"https://data.gharchive.org/$daily\"; then\n"
            "    hdfs dfs -put -f \"$daily\" /user/root/gharchive/\n"
            "    rm -f \"$daily\"\n"
            "    continue\n"
            "  fi\n"
            "  rm -f \"$daily\"\n"
            "  got_any=0\n"
            "  for hour in $(seq 0 23); do\n"
            "    hourly=\"$day-$hour.json.gz\"\n"
            "    if wget -q -O \"$hourly\" \"https://data.gharchive.org/$hourly\"; then\n"
            "      hdfs dfs -put -f \"$hourly\" /user/root/gharchive/\n"
            "      rm -f \"$hourly\"\n"
            "      got_any=1\n"
            "    else\n"
            "      rm -f \"$hourly\"\n"
            "    fi\n"
            "  done\n"
            "  if [ \"$got_any\" -eq 0 ]; then\n"
            "    echo \"No GHArchive file found for $day\" >&2\n"
            "  fi\n"
            "done\n"
            "hdfs dfs -count /user/root/gharchive\n"
            "'"
        ),
    )

    check_hdfs_input = BashOperator(
        task_id="check_hdfs_input",
        bash_command=(
            "docker exec hadoop-master hdfs dfs -test -e /user/root/gharchive && "
            "docker exec hadoop-master hdfs dfs -count /user/root/gharchive"
        ),
    )

    run_spark_batch = BashOperator(
        task_id="run_spark_batch",
        bash_command=(
            "docker exec opentrend-spark "
            "spark-submit --master local[*] /app/batch_job.py"
        ),
    )

    check_runtime_dependencies >> ingest_last_7_days_to_hdfs >> check_hdfs_input >> run_spark_batch
