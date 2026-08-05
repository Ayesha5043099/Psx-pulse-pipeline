# PSX Pulse  Real-Time Pakistan Stock Exchange Intelligence Pipeline

An end-to-end data engineering pipeline that ingests real-time Pakistan Stock Exchange (PSX) prices, PKR/USD exchange rates, and financial news, then processes, stores, transforms, orchestrates, and visualizes the data through a modern, production-style stack.

Unlike common portfolio projects built on NYC taxi or generic Kaggle datasets, this pipeline is built around real, local Pakistani financial data combining market prices, currency movement, and news sentiment into a single analytics-ready dashboard.

---

## Architecture

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
│
▼
Apache Superset (interactive BI dashboard)
---

## Tech Stack

| Layer            | Tool                                          |
|-------------------|------------------------------------------------|
| Ingestion          | Python (`requests`, `feedparser`, `psxdata`)   |
| Streaming          | Apache Kafka                                   |
| Processing         | Python (Kafka Consumer, pandas)                |
| Data Lake          | MinIO (S3-compatible)                          |
| Warehouse          | Snowflake                                      |
| Transformation     | dbt                                            |
| Orchestration      | Apache Airflow                                 |
| Visualization      | Apache Superset                                |
| Infrastructure     | Docker Compose                                 |

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
5. **Orchestration** — An Airflow DAG runs the three ingestion tasks in parallel, followed by processing and warehouse loading, on a daily schedule — fully automated end to end.
6. **Visualization** — Apache Superset connects directly to Snowflake, powering an interactive dashboard with dynamic filters, KPI summaries, and multiple chart types (bar, scatter, bubble, treemap, and data tables).

---

## Dashboard

The Superset dashboard brings the entire dataset together in one interactive view:

- Dynamic stock and sector filters that update every chart in real time
- Stock price comparison across all tracked symbols
- Market sentiment breakdown by stock (derived from news analysis)
- PE ratio and dividend yield comparison
- Sentiment vs. price change correlation, highlighting the core hypothesis of the project — whether news sentiment moves alongside stock performance
- Live PKR/USD exchange rate as a headline KPI

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

# Start Kafka, MinIO, Airflow, and Superset
cd docker
docker-compose up -d --build

# Access services
# Airflow UI:      http://localhost:8084  (admin/admin)
# Kafka UI:        http://localhost:8082
# MinIO Console:   http://localhost:9003
# Superset:        http://localhost:8088  (admin/admin)

# Run dbt models
cd ../dbt_project
dbt run
dbt test
```

---

## Roadmap

- [x] Real-time ingestion (PSX, FX, news)
- [x] Kafka streaming
- [x] Processing + sentiment scoring
- [x] Data lake (MinIO)
- [x] Warehouse (Snowflake)
- [x] dbt transformation + testing
- [x] Airflow orchestration (fully automated)
- [x] Interactive BI dashboard (Apache Superset)

---

