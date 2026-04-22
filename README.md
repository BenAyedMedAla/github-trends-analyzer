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
 
### 3. Load batch data into HDFS (optional fallback)

```bash
# inside hadoop-master container (example: one full day, hourly files)
hdfs dfs -mkdir -p /user/root/gharchive
for h in {0..23}; do
	wget https://data.gharchive.org/2024-01-15-$h.json.gz
done
hdfs dfs -put -f 2024-01-15-*.json.gz /user/root/gharchive/
```
 
### 4. Start everything
 
```bash
docker compose up --build
```

Start Airflow (scheduler + web UI):

```bash
docker compose --profile airflow up -d airflow
```

Airflow UI: http://localhost:8080
Default credentials: `admin` / `admin`

### 5. Run the weekly batch manually (HDFS input only)

After GH Archive files are present under `hdfs:///user/root/gharchive/`, run:

```bash
docker compose --profile batch run --rm spark-batch
```

The batch job writes into HBase tables used by Streamlit:
- `weekly_metrics`
- `ml_predictions`

### 6. Airflow orchestration (recommended)

An example DAG is provided at `airflow/dags/hdfs_weekly_batch.py`.

It:
- checks runtime dependency (`docker`)
- downloads the last 7 full days of GH Archive data
- uploads downloaded files into `/user/root/gharchive` in HDFS
- verifies HDFS input exists at `/user/root/gharchive`
- triggers `spark-submit` in `opentrend-spark`
- runs weekly on Monday at 02:00

Use Airflow as the single scheduler and remove duplicate schedulers.

To run it once manually from Airflow UI:
- open DAG `hdfs_weekly_batch`
- click `Trigger DAG`

This trigger now performs full automation:
- ingestion (GH Archive -> HDFS)
- weekly Spark batch execution
 
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
 
