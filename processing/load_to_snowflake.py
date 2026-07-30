import os
import io
import pandas as pd
from minio import Minio
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables (Docker env vars take priority over .env)
load_dotenv(dotenv_path="../configs/.env")

# --- MinIO config ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9002")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "psx-data-lake")

# --- Snowflake config ---
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "compute_wh")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "psx_pulse_db")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "raw_data")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "accountadmin")

# Maps MinIO folder names to Snowflake table names
FOLDER_TABLE_MAP = {
    "psx_prices": "PSX_PRICES",
    "fx_rates": "FX_RATES",
    "news_articles": "NEWS_ARTICLES",
}


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def get_latest_parquet_object(client, folder_name):
    """Finds the most recently created Parquet file in a bronze/<folder_name>/ path"""
    prefix = f"bronze/{folder_name}/"
    objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))

    if not objects:
        return None

    latest = max(objects, key=lambda obj: obj.last_modified)
    return latest.object_name


def read_parquet_from_minio(client, object_name):
    """Reads a Parquet object from MinIO into a pandas DataFrame"""
    response = client.get_object(MINIO_BUCKET, object_name)
    buffer = io.BytesIO(response.read())
    response.close()
    response.release_conn()
    return pd.read_parquet(buffer)


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )


def load_dataframe_to_snowflake(conn, df, table_name):
    """Loads a DataFrame into Snowflake, replacing the table's contents each run"""
    if df.empty:
        print(f" No data to load for {table_name}")
        return

    # Snowflake convention: uppercase column names
    df.columns = [c.upper() for c in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table_name,
        auto_create_table=True,
        overwrite=True
    )
    print(f" Loaded {nrows} rows into Snowflake table: {table_name}")


def main():
    minio_client = get_minio_client()
    conn = get_snowflake_connection()

    print(" Starting load: MinIO → Snowflake\n")

    for folder_name, table_name in FOLDER_TABLE_MAP.items():
        object_name = get_latest_parquet_object(minio_client, folder_name)

        if object_name is None:
            print(f" No files found for {folder_name}, skipping")
            continue

        print(f" Reading latest file for {folder_name}: {object_name}")
        df = read_parquet_from_minio(minio_client, object_name)
        load_dataframe_to_snowflake(conn, df, table_name)

    conn.close()
    print("\n Load complete")


if __name__ == "__main__":
    main()