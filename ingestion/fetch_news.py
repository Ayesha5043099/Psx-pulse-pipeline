import feedparser
import json
from datetime import datetime
import os

# Pakistani financial news RSS feeds
RSS_FEEDS = {
    "Dawn_Business": "https://www.dawn.com/feeds/business",
}

def fetch_news():
    """Fetches financial news headlines from RSS feeds"""
    all_articles = []
    
    for source_name, feed_url in RSS_FEEDS.items():
        try:
            feed = feedparser.parse(feed_url)
            
            if feed.bozo and not feed.entries:
                print(f" {source_name}: could not parse feed")
                continue
            
            for entry in feed.entries[:15]:  # Only latest 15 articles
                article = {
                    "source": source_name,
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "summary": entry.get("summary", "")[:300],  # Keep summary short
                    "fetched_at": datetime.now().isoformat()
                }
                all_articles.append(article)
            
            print(f" {source_name}: {len(feed.entries[:15])} articles fetched")
            
        except Exception as e:
            print(f" Error fetching {source_name}: {e}")
    
    return all_articles

def save_to_file(data):
    """Saves data to a JSON file"""
    if not data:
        print(" No data found, file not saved")
        return
    
    os.makedirs("../data/raw", exist_ok=True)
    filename = f"../data/raw/news_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f" Data saved to: {filename}")

if __name__ == "__main__":
    print(" Fetching financial news...\n")
    news_data = fetch_news()
    save_to_file(news_data)
    print(f"\n Total {len(news_data)} articles collected")