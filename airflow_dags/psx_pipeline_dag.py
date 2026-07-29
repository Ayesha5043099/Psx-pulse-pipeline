from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "ayesha",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="psx_pulse_pipeline",
    default_args=default_args,
    description="Fetch PSX stocks, FX rate, and news, then process into MinIO",
    start_date=datetime(2026, 7, 27),
    schedule_interval="@daily",   # Runs once a day; change to "*/30 * * * *" for every 30 min
    catchup=False,
    tags=["psx-pulse"],
) as dag:

    fetch_psx = BashOperator(
        task_id="fetch_psx_stock_data",
        bash_command="cd /opt/airflow/ingestion && python3 fetch_psx_data.py"
    )

    fetch_fx = BashOperator(
        task_id="fetch_fx_rate",
        bash_command="cd /opt/airflow/ingestion && python3 fetch_fx_rate.py"
    )

    fetch_news = BashOperator(
        task_id="fetch_news_articles",
        bash_command="cd /opt/airflow/ingestion && python3 fetch_news.py"
    )

    process_data = BashOperator(
        task_id="process_and_store_to_minio",
        bash_command="cd /opt/airflow/processing && python3 process_and_store.py"
    )

    # Task dependency: run all three fetchers in parallel, then process
    [fetch_psx, fetch_fx, fetch_news] >> process_data