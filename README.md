# github-trends-analyzer
> A lightweight big data pipeline that detects trending open-source repositories in real time using stream and batch processing.
 
Built as a university big data project. Combines a live GitHub event stream with daily historical analysis to surface which repositories are gaining momentum — right now and over time.
 
---
 
## What it does
 
- **Stream side** — polls the GitHub Events API every 5 seconds, pushes events through Kafka, and uses Spark Structured Streaming to compute trending repositories in 5-minute windows
- **Batch side** — reads the previous day's GitHub Archive data every day at 02:00, runs PySpark to compute star velocity and rising languages, and scores repositories with a machine learning model
- **Dashboard** — a streamlit app showing 6 live panels: trending now repos, rising languages, live event feed, historical trends, trending yesterday repos, and AI-predicted breakouts
 
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
git clone https://github.com/BenAyedMedAla/github-trends-analyzer
cd github-trends-analyzer
```
 
### 2. Set up your environment
 
Copy the example env file and add your GitHub tokens:
 
```bash
cp .env.example .env
```
 
Edit `.env`:
 
```env
GITHUB_TOKEN_STREAMING=ghp_your_streaming_token
GITHUB_TOKEN_BATCH=ghp_your_batch_token
```
 
A GitHub personal access token is free — generate one at github.com/settings/tokens with no scopes required. It raises your API limit from 60 to 5,000 requests/hour.

Optional: keep `GITHUB_TOKEN` as a shared fallback token if you do not want to set both variables.
 
### 3. Start Hadoop, Kafka, and HBase

If the Hadoop containers are not running yet, start them:

```bash
docker run -itd --net=hadoop \
	-p 9870:9870 -p 8088:8088 -p 7077:7077 \
	-p 16010:16010 -p 9092:9092 -p 16000:16000 \
	--name hadoop-master --hostname hadoop-master \
	liliasfaxi/hadoop-cluster:latest

docker run -itd --net=hadoop -p 8040:8042 \
	--name hadoop-worker1 --hostname hadoop-worker1 \
	liliasfaxi/hadoop-cluster:latest

docker run -itd --net=hadoop -p 8041:8042 \
	--name hadoop-worker2 --hostname hadoop-worker2 \
	liliasfaxi/hadoop-cluster:latest
```

Then enter the Hadoop master container:

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
create 'batch_metadata', 'meta'
```

List the tables to confirm:

```hbase
list
```

You should see:

- `live_events`
- `live_metrics`
- `repos`
- `weekly_metrics`
- `ml_predictions`
- `batch_metadata`

Exit HBase shell with:

```hbase
exit
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
Default credentials: you get them from the airflow container logs

### 8. Run the daily batch manually from Airflow

After Airflow starts, you can trigger the batch DAG from the UI or by command.

Trigger from the Airflow UI:

- open DAG `hdfs_daily_batch`
- click `Trigger DAG`

Trigger from the terminal:

```bash
docker exec opentrend-airflow airflow dags trigger hdfs_daily_batch
```

The DAG will:
- download the previous day GH Archive data only
- upload them into HDFS at `/user/root/gharchive`
- verify the HDFS input exists
- run the Spark batch job

The batch job writes into HBase tables used by Streamlit:
- `batch_metadata`
- `ml_predictions`

### 9. Useful commands to watch the batch

Check DAG runs:

```bash
docker exec opentrend-airflow airflow dags list-runs -d hdfs_daily_batch --no-backfill
```

Check task states for a specific run:

```bash
docker exec opentrend-airflow airflow tasks states-for-dag-run hdfs_daily_batch "manual__YYYY-MM-DDTHH:MM:SS+00:00"
```

Tail the ingestion task log:

```bash
docker exec opentrend-airflow bash -lc "tail -n 80 /opt/airflow/logs/dag_id=hdfs_daily_batch/run_id=manual__YYYY-MM-DDTHH:MM:SS+00:00/task_id=ingest_previous_day_to_hdfs/attempt=1.log"
```

Tail the Spark batch task log:

```bash
docker exec opentrend-airflow bash -lc "tail -n 80 /opt/airflow/logs/dag_id=hdfs_daily_batch/run_id=manual__YYYY-MM-DDTHH:MM:SS+00:00/task_id=run_spark_batch/attempt=1.log"
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
scan 'batch_metadata'
scan 'ml_predictions'
exit
```

### 10. Airflow orchestration details

An example DAG is provided at `airflow/dags/hdfs_daily_batch.py`.

It:
- checks runtime dependency (`docker`)
- downloads the previous day GH Archive data only
- uploads downloaded files into `/user/root/gharchive` in HDFS
- verifies HDFS input exists at `/user/root/gharchive`
- triggers `spark-submit` in `opentrend-spark` (which processes previous day files only)
- runs daily at 02:00

Use Airflow as the single scheduler and remove duplicate schedulers.

The DAG is configured for previous-day processing by default.

Optional override: set `BATCH_DAY=YYYY-MM-DD` in the Spark environment to backfill a specific day.
 
| Service | URL |
|---|---|
| Dashboard | http://localhost:8501 | 
---
 
## Dashboard panels
 
| Panel | Source | Updates |
|---|---|---|
| Trending now | `live_metrics` | every 10 min via stream |
| Rising languages | `weekly_metrics` | daily via batch |
| Live activity feed | `events_stream` | every few seconds |
| Historical trends | `weekly_metrics` | daily via batch |
| AI insights | `ml_predictions` | daily via batch |
 
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
 
