import psxdata
import json
from datetime import datetime
from kafka import KafkaProducer

# Stocks to collect data for
SYMBOLS = ["LUCK", "HBL", "ENGRO", "OGDC", "PSO"]

# Kafka producer setup — connects to the broker exposed to Windows (host)
producer = KafkaProducer(
    bootstrap_servers="localhost:9093",
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
            print(f" {symbol} published to Kafka topic 'psx_prices'")

        except Exception as e:
            print(f" Error fetching/publishing {symbol}: {e}")

    producer.flush()  # Ensure all messages are sent before exiting
    print("\n All messages flushed to Kafka")

if __name__ == "__main__":
    print(" Starting PSX data fetch and publish...\n")
    fetch_and_publish()