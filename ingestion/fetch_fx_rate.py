import requests
import json
from datetime import datetime
import os

API_URL = "https://open.er-api.com/v6/latest/USD"

def fetch_fx_rate():
    """Fetches the PKR exchange rate based on USD"""
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
        
        print(f"Current rate: 1 USD = {pkr_rate} PKR")
        return result
        
    except Exception as e:
        print(f" Error fetching FX rate: {e}")
        return None

def save_to_file(data):
    """Saves data to a JSON file"""
    if data is None:
        print(" No data found, file not saved")
        return
    
    os.makedirs("../data/raw", exist_ok=True)
    filename = f"../data/raw/fx_rate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f" Data saved to: {filename}")

if __name__ == "__main__":
    print(" Fetching PKR/USD exchange rate...\n")
    fx_data = fetch_fx_rate()
    save_to_file(fx_data)