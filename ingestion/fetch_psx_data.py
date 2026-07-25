import psxdata
import json
from datetime import datetime
import os

# Jin stocks ka data collect karna hai
SYMBOLS = ["LUCK", "HBL", "ENGRO", "OGDC", "PSO"]

def fetch_stock_data():
    """PSX se stocks ka current data fetch karta hai"""
    results = []
    
    for symbol in SYMBOLS:
        try:
            quote = psxdata.quote(symbol)
            # DataFrame ko dictionary mein convert karo
            data = quote.to_dict(orient="records")[0]
            data["fetched_at"] = datetime.now().isoformat()
            results.append(data)
            print(f" {symbol} data fetch ho gaya")
        except Exception as e:
            print(f" {symbol} fetch karte waqt error: {e}")
    
    return results

def save_to_file(data):
    """Data ko JSON file mein save karta hai"""
    os.makedirs("../data/raw", exist_ok=True)
    filename = f"../data/raw/psx_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"\n📁 Data save ho gaya: {filename}")

if __name__ == "__main__":
    print(" PSX data fetch shuru ho raha hai...\n")
    stock_data = fetch_stock_data()
    save_to_file(stock_data)
    print(f"\n Total {len(stock_data)} stocks ka data collect ho gaya")