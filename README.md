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
| Orchestration | Docker Compose |
 
 
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
 
### 3. Download some batch data (optional, for batch panels)
 
```bash
# example: download one week of data
wget https://data.gharchive.org/2024-01-15-{0..23}.json.gz -P data/gharchive/
```
 
### 4. Start everything
 
```bash
docker compose up --build
```

### 5. Run the weekly batch manually (using local `.json.gz` files)

If you downloaded GH Archive files into `data/gharchive/`, run:

```bash
docker compose --profile batch run --rm spark-batch
```

The batch job writes into HBase tables used by Streamlit:
- `weekly_metrics`
- `ml_predictions`

### 6. One-click batch from Streamlit

In the Streamlit sidebar, click **Run Weekly Batch**.

What it does automatically:
- downloads the last 7 full days of GH Archive files into `data/gharchive/`
- triggers Spark batch processing
- writes fresh data to `weekly_metrics` and `ml_predictions`
- shows execution logs in the app
 
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
 
