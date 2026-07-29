import os
import feedparser
import json
from datetime import datetime
from kafka import KafkaProducer

RSS_FEEDS = {
    "Dawn_Business": "https://www.dawn.com/feeds/business",
}

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_publish():
    """Fetches news headlines and publishes them to the Kafka topic"""
    total_published = 0

    for source_name, feed_url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)

            if feed.bozo and not feed.entries:
                print(f"❌ {source_name}: could not parse feed")
                continue

            for entry in feed.entries[:15]:
                article = {
                    "source": source_name,
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "summary": entry.get("summary", "")[:300],
                    "fetched_at": datetime.now().isoformat()
                }
                producer.send("news_articles", value=article)
                total_published += 1

            print(f"✅ {source_name}: {len(feed.entries[:15])} articles published")

        except Exception as e:
            print(f"❌ Error fetching/publishing {source_name}: {e}")

    producer.flush()
    print(f"\n📤 Total {total_published} articles flushed to Kafka")

if __name__ == "__main__":
    print(f"🚀 Starting news fetch and publish... (Kafka: {KAFKA_SERVER})\n")
    fetch_and_publish()