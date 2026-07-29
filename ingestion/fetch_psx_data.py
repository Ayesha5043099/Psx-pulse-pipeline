import os
import psxdata
import json
from datetime import datetime
from kafka import KafkaProducer

# Stocks to collect data for
SYMBOLS = ["LUCK", "HBL", "ENGRO", "OGDC", "PSO"]

# Kafka server address — uses container address inside Airflow,
# falls back to host address when run locally
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

def fetch_and_publish():
    """Fetches PSX stock data and publishes it to the Kafka topic"""
    for symbol in SYMBOLS:
        try:
            quote = psxdata.quote(symbol)
            data = quote.to_dict(orient="records")[0]
            data["fetched_at"] = datetime.now().isoformat()

            producer.send("psx_prices", value=data)
            print(f"✅ {symbol} published to Kafka topic 'psx_prices'")

        except Exception as e:
            print(f"❌ Error fetching/publishing {symbol}: {e}")

    producer.flush()  # Ensure all messages are sent before exiting
    print("\n📤 All messages flushed to Kafka")

if __name__ == "__main__":
    print(f"🚀 Starting PSX data fetch and publish... (Kafka: {KAFKA_SERVER})\n")
    fetch_and_publish()