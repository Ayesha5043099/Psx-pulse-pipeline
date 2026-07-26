import json
from kafka import KafkaConsumer

# List of topics to listen to
TOPICS = ["psx_prices", "fx_rates", "news_articles"]

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers="localhost:9093",
    auto_offset_reset="earliest",   # Read from the beginning of the topic
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000       # Stop after 10 seconds of no new messages
)

print(" Listening to Kafka topics: psx_prices, fx_rates, news_articles\n")
print("(Waiting for messages... will stop automatically after 10s of silence)\n")

message_count = 0

for message in consumer:
    message_count += 1
    print(f" [{message.topic}] Message #{message_count}:")
    print(json.dumps(message.value, indent=2))
    print("-" * 50)

print(f"\n Total {message_count} messages consumed")