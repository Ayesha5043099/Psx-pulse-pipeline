
# PSX Pulse  Real-Time Pakistan Stock Exchange Intelligence Pipeline

An end-to-end data engineering pipeline that ingests real-time Pakistan Stock Exchange (PSX) prices, PKR/USD exchange rates, and financial news, then processes, stores, transforms, and orchestrates the data through a modern, production-style stack.

Unlike common portfolio projects built on NYC taxi or generic Kaggle datasets, this pipeline is built around real, local Pakistani financial data  combining market prices, currency movement, and news sentiment into a single analytics-ready table.

---

## Architecture

```
Data Sources (PSX stocks, PKR/USD rate, Dawn Business news)
        │
        ▼
   Kafka Producers (Python)
        │
        ▼
   Apache Kafka (streaming broker)
        │
        ▼
Processing Layer (Kafka Consumer + sentiment scoring)
        │
        ▼
   MinIO (S3-compatible data lake, Parquet format)
        │
        ▼
   Snowflake (cloud data warehouse)
        │
        ▼
   dbt (staging models → tested mart model)
        │
        ▼
Apache Airflow (orchestrates the entire pipeline daily)
```

---

## Tech Stack

| Layer            | Tool                                   |
|-------------------|-----------------------------------------|
| Ingestion          | Python (`requests`, `feedparser`, `psxdata`) |
| Streaming          | Apache Kafka                            |
| Processing         | Python (Kafka Consumer, pandas)         |
| Data Lake          | MinIO (S3-compatible)                   |
| Warehouse          | Snowflake                               |
| Transformation     | dbt                                     |
| Orchestration      | Apache Airflow                          |
| Infrastructure     | Docker Compose                          |

---

## Data Sources

- PSX stock prices — via the [`psxdata`](https://pypi.org/project/psxdata/) Python library (LUCK, HBL, ENGRO, OGDC, PSO)
- PKR/USD exchange rate — [open.er-api.com](https://open.er-api.com)
- Financial news — Dawn Business RSS feed, with lightweight keyword-based sentiment scoring

---

## How It Works

1. **Ingestion** — Three Python scripts fetch stock prices, FX rate, and news headlines, publishing each to its own Kafka topic (`psx_prices`, `fx_rates`, `news_articles`).
2. **Processing** — A Kafka consumer reads all three topics, scores news sentiment, and writes the results to MinIO as partitioned Parquet files (bronze layer).
3. **Warehousing** — A loader script reads the latest Parquet files from MinIO and loads them into Snowflake raw tables.
4. **Transformation** — dbt staging models clean and deduplicate the raw data; a mart model (`stock_sentiment_summary`) joins stock prices, FX rate, and average market sentiment into one analysis-ready table. 10 dbt tests validate data quality (not-null, uniqueness).
5. **Orchestration** — An Airflow DAG runs the three ingestion tasks in parallel, followed by the processing task, on a daily schedule.

---

## Challenges & Fixes

Building this pipeline surfaced several real-world data engineering problems:

- **Container networking** — Services referencing `localhost` worked from the host machine but failed inside Docker containers; fixed by using Docker service names (`kafka:9092`, `minio:9000`) via environment variables, with `localhost` as a fallback for local runs.
- **Python version mismatch** — The default Airflow image (Python 3.8) couldn't install a library requiring Python 3.11+; solved by switching to the `apache/airflow:2.8.1-python3.11` image and baking dependencies into a custom Dockerfile so they persist across container rebuilds.
- **Duplicate records** — Because the Kafka consumer had no consumer group tracking, repeated pipeline runs re-read the full topic history, creating duplicate rows in Snowflake. Fixed with a `ROW_NUMBER()` deduplication window function in the dbt staging layer, keeping only the latest record per stock symbol.
- **Airflow scheduler instability** — Running too many unrelated Docker projects simultaneously exhausted system resources and crashed the scheduler; resolved by stopping unused containers from other projects.

---

## Running Locally

**Prerequisites:** WSL2 (or Linux), Docker Desktop, Python 3.11+, a free [Snowflake trial account](https://signup.snowflake.com)

```bash
# Clone the repo
git clone https://github.com/Ayesha5043099/Psx-pulse-pipeline.git
cd Psx-pulse-pipeline

# Set up Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment variables
cp configs/.env.example configs/.env
# then edit configs/.env with your MinIO / Snowflake credentials

# Start Kafka, MinIO, and Airflow
cd docker
docker-compose up -d --build

# Access services
# Airflow UI:   http://localhost:8084  (admin/admin)
# Kafka UI:     http://localhost:8082
# MinIO Console: http://localhost:9003

# Run dbt models
cd ../dbt_project
dbt run
dbt test
```





