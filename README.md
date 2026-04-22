# github-trends-analyzer
> A lightweight big data pipeline that detects trending open-source repositories in real time using stream and batch processing.
 
Built as a university big data project. Combines a live GitHub event stream with weekly historical analysis to surface which repositories are gaining momentum — right now and over time.
 
---
 
## What it does
 
- **Stream side** — polls the GitHub Events API every 60 seconds, pushes events through Kafka, and uses Spark Structured Streaming to compute trending repositories in 10-minute windows
- **Batch side** — reads weekly GitHub Archive data, runs PySpark to compute star velocity and rising languages, and scores repositories with a machine learning model
- **Dashboard** — a streamlit app showing 6 live panels: trending now repos, rising languages, live event feed, historical trends, trending this week repos, and AI-predicted breakouts
 
---
 
## Stack
 
| Layer | Technology |
|---|---|
| Stream ingestion | Python producer + Apache Kafka |
| Stream processing | Apache Spark Structured Streaming |
| Batch processing | PySpark reading GH Archive `.json.gz` files |
| Storage | HBASE |
|  Dashboard | Streamlit |
| Orchestration | Apache Airflow + Docker Compose |
 
 
## Getting started
 
### 1. Clone the repo
 
```bash
git clone https://github.com/your-username/opentrend.git
cd opentrend
```
 
### 2. Set up your environment
 
Copy the example env file and add your GitHub token:
 
```bash
cp .env.example .env
```
 
Edit `.env`:
 
```env
GITHUB_TOKEN=ghp_yourtoken
```
 
A GitHub personal access token is free — generate one at github.com/settings/tokens with no scopes required. It raises your API limit from 60 to 5,000 requests/hour.
 
### 3. Start Hadoop, Kafka, and HBase

Start the Hadoop master container first:

```bash
docker exec -it hadoop-master bash
```

Inside `hadoop-master`, start the services in this order:

```bash
./start-hadoop.sh
./start-kafka-zookeeper.sh
./start-kafka-zookeeper.sh
start-hbase.sh
hbase-daemon.sh start thrift
```

Check the processes with `jps`. You should see:

- `NameNode`
- `SecondaryNameNode`
- `ResourceManager`
- `Kafka`
- `QuorumPeerMain`
- `HMaster`
- `HRegionServer`
- `ThriftServer`

### 4. Create the Kafka topic

Still inside `hadoop-master`, create the `github-events` topic:

```bash
kafka-topics.sh --create --topic github-events --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092
```

List topics to confirm it exists:

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

You should see:

- `github-events`

### 5. Create the HBase tables

Open the HBase shell:

```bash
hbase shell
```

Create the tables used by the batch and dashboard:

```hbase
create 'live_events', 'event'
create 'live_metrics', 'repo', 'metrics'
create 'repos', 'info'
create 'weekly_metrics', 'repo', 'stats'
create 'ml_predictions', 'repo', 'ml'
```

List the tables to confirm:

```hbase
list
```

You should see at least:

- `live_events`
- `live_metrics`
- `repos`
- `weekly_metrics`
- `ml_predictions`

Exit HBase shell with:

```hbase
exit
```

### 6. Load batch data into HDFS (optional fallback)

```bash
# inside hadoop-master container (example: one full day, hourly files)
hdfs dfs -mkdir -p /user/root/gharchive
for h in {0..23}; do
	wget https://data.gharchive.org/2024-01-15-$h.json.gz
done
hdfs dfs -put -f 2024-01-15-*.json.gz /user/root/gharchive/
```
 
### 7. Start the app containers
 
```bash
docker compose up --build
```

Start Airflow (scheduler + web UI):

```bash
docker compose --profile airflow up -d airflow
```

Airflow UI: http://localhost:8080
Default credentials: `admin` / `admin`

### 8. Run the weekly batch manually from Airflow

After Airflow starts, you can trigger the batch DAG from the UI or by command.

Trigger from the Airflow UI:

- open DAG `hdfs_weekly_batch`
- click `Trigger DAG`

Trigger from the terminal:

```bash
docker exec opentrend-airflow airflow dags trigger hdfs_weekly_batch
```

The DAG will:
- download the last 7 full days of GH Archive data
- upload them into HDFS at `/user/root/gharchive`
- verify the HDFS input exists
- run the Spark batch job

The batch job writes into HBase tables used by Streamlit:
- `weekly_metrics`
- `ml_predictions`

### 9. Useful commands to watch the batch

Check DAG runs:

```bash
docker exec opentrend-airflow airflow dags list-runs -d hdfs_weekly_batch --no-backfill
```

Check task states for a specific run:

```bash
docker exec opentrend-airflow airflow tasks states-for-dag-run hdfs_weekly_batch "manual__YYYY-MM-DDTHH:MM:SS+00:00"
```

Tail the ingestion task log:

```bash
docker exec opentrend-airflow bash -lc "tail -n 80 /opt/airflow/logs/dag_id=hdfs_weekly_batch/run_id=manual__YYYY-MM-DDTHH:MM:SS+00:00/task_id=ingest_last_7_days_to_hdfs/attempt=1.log"
```

Tail the Spark batch task log:

```bash
docker exec opentrend-airflow bash -lc "tail -n 80 /opt/airflow/logs/dag_id=hdfs_weekly_batch/run_id=manual__YYYY-MM-DDTHH:MM:SS+00:00/task_id=run_spark_batch/attempt=1.log"
```

Watch Spark container output directly:

```bash
docker logs -f opentrend-spark
```

Watch Airflow output directly:

```bash
docker logs -f opentrend-airflow
```

Check HBase tables after the run:

```bash
hbase shell
scan 'weekly_metrics'
scan 'ml_predictions'
exit
```

### 10. Airflow orchestration details

An example DAG is provided at `airflow/dags/hdfs_weekly_batch.py`.

It:
- checks runtime dependency (`docker`)
- downloads the last 7 full days of GH Archive data
- uploads downloaded files into `/user/root/gharchive` in HDFS
- verifies HDFS input exists at `/user/root/gharchive`
- triggers `spark-submit` in `opentrend-spark`
- runs weekly on Monday at 02:00

Use Airflow as the single scheduler and remove duplicate schedulers.

If you want a faster test run, set `GHARCHIVE_DAYS_BACK=1` in the Airflow container environment before starting Airflow. The default is `7`.
 
| Service | URL |
|---|---|
| Dashboard | http://localhost:8501 | 
---
 
## Dashboard panels
 
| Panel | Source | Updates |
|---|---|---|
| Trending now | `live_metrics` | every 10 min via stream |
| Rising languages | `weekly_metrics` | weekly via batch |
| Live activity feed | `events_stream` | every few seconds |
| Historical trends | `weekly_metrics` | weekly via batch |
| AI insights | `ml_predictions` | weekly via batch |
 
---
 
## Architecture
 

 
---
 
## Requirements
 
- Docker Desktop (4 GB RAM allocated minimum, 6 GB recommended)
- A free GitHub personal access token
- ~500 MB disk space for base images
 
---
 
## License
 
MIT
 
