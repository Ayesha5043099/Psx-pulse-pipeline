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
    description="Fetch PSX stocks, FX rate, and news, process into MinIO, then load into Snowflake",
    start_date=datetime(2026, 7, 27),
    schedule_interval="@daily",
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

    load_to_warehouse = BashOperator(
        task_id="load_to_snowflake",
        bash_command="cd /opt/airflow/processing && python3 load_to_snowflake.py"
    )

    # Fetch tasks run in parallel, then process, then load into Snowflake
    [fetch_psx, fetch_fx, fetch_news] >> process_data >> load_to_warehouse