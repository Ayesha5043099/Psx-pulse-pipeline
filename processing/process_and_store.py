import json
import os
import io
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer
from minio import Minio
from dotenv import load_dotenv

# Load environment variables from configs/.env
load_dotenv(dotenv_path="../configs/.env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

TOPICS = ["psx_prices", "fx_rates", "news_articles"]

# --- Simple sentiment scoring (lightweight, no heavy ML model needed) ---
POSITIVE_WORDS = ["growth", "profit", "rise", "gain", "surge", "record", "boost", "recovery"]
NEGATIVE_WORDS = ["loss", "decline", "fall", "crisis", "drop", "concern", "inflation", "pressure"]

def simple_sentiment(text):
    """Basic keyword-based sentiment score between -1 and 1"""
    if not text:
        return 0.0
    text_lower = text.lower()
    pos_count = sum(word in text_lower for word in POSITIVE_WORDS)
    neg_count = sum(word in text_lower for word in NEGATIVE_WORDS)
    total = pos_count + neg_count
    if total == 0:
        return 0.0
    return round((pos_count - neg_count) / total, 2)


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # local setup, no HTTPS
    )


def ensure_bucket(client):
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f" Bucket '{MINIO_BUCKET}' created")


def save_dataframe_to_minio(client, df, folder_name):
    """Saves a DataFrame as a Parquet file to MinIO under bronze/<folder_name>/"""
    if df.empty:
        print(f" No data to save for {folder_name}")
        return

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"bronze/{folder_name}/{folder_name}_{timestamp}.parquet"

    client.put_object(
        MINIO_BUCKET,
        object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )
    print(f" Saved {len(df)} rows to MinIO: {object_name}")


def consume_and_process():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000
    )

    minio_client = get_minio_client()
    ensure_bucket(minio_client)

    buckets = {
        "psx_prices": [],
        "fx_rates": [],
        "news_articles": []
    }

    print(" Reading messages from Kafka topics...\n")

    for message in consumer:
        buckets[message.topic].append(message.value)

    # --- Process stock prices ---
    if buckets["psx_prices"]:
        df_stocks = pd.DataFrame(buckets["psx_prices"])
        save_dataframe_to_minio(minio_client, df_stocks, "psx_prices")

    # --- Process FX rates ---
    if buckets["fx_rates"]:
        df_fx = pd.DataFrame(buckets["fx_rates"])
        save_dataframe_to_minio(minio_client, df_fx, "fx_rates")

    # --- Process news with sentiment ---
    if buckets["news_articles"]:
        df_news = pd.DataFrame(buckets["news_articles"])
        df_news["sentiment_score"] = (df_news["title"] + " " + df_news["summary"]).apply(simple_sentiment)
        save_dataframe_to_minio(minio_client, df_news, "news_articles")

    print(f"\n Processing complete. Totals — stocks: {len(buckets['psx_prices'])}, "
          f"fx: {len(buckets['fx_rates'])}, news: {len(buckets['news_articles'])}")


if __name__ == "__main__":
    print(" Starting processing job (Kafka → Transform → MinIO)...\n")
    consume_and_process()