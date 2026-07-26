import requests
import json
from datetime import datetime
from kafka import KafkaProducer

API_URL = "https://open.er-api.com/v6/latest/USD"

producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_publish():
    """Fetches PKR exchange rate and publishes it to the Kafka topic"""
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        pkr_rate = data["rates"]["PKR"]

        result = {
            "base_currency": "USD",
            "target_currency": "PKR",
            "rate": pkr_rate,
            "last_updated": data.get("time_last_update_utc"),
            "fetched_at": datetime.now().isoformat()
        }

        producer.send("fx_rates", value=result)
        producer.flush()

        print(f" Published: 1 USD = {pkr_rate} PKR")

    except Exception as e:
        print(f" Error fetching/publishing FX rate: {e}")

if __name__ == "__main__":
    print(" Starting FX rate fetch and publish...\n")
    fetch_and_publish()