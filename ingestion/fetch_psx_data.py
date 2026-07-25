import psxdata
import json
from datetime import datetime
import os

# Stocks to collect data for
SYMBOLS = ["LUCK", "HBL", "ENGRO", "OGDC", "PSO"]

def fetch_stock_data():
    """Fetches current data for PSX stocks"""
    results = []
    
    for symbol in SYMBOLS:
        try:
            quote = psxdata.quote(symbol)
            # Convert DataFrame to dictionary
            data = quote.to_dict(orient="records")[0]
            data["fetched_at"] = datetime.now().isoformat()
            results.append(data)
            print(f" {symbol} data fetched successfully")
        except Exception as e:
            print(f" Error fetching {symbol}: {e}")
    
    return results

def save_to_file(data):
    """Saves data to a JSON file"""
    os.makedirs("../data/raw", exist_ok=True)
    filename = f"../data/raw/psx_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"\n Data saved to: {filename}")

if __name__ == "__main__":
    print(" Starting PSX data fetch...\n")
    stock_data = fetch_stock_data()
    save_to_file(stock_data)
    print(f"\n Total {len(stock_data)} stocks collected")