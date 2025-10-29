import os
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("europe-econ-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)
os.chdir(CURRENT_DIR)
logging.info(f"Working directory set to: {CURRENT_DIR}")

API_KEY = os.getenv("FRED_API_KEY")
RAW_DIR = os.path.join(CURRENT_DIR, "data/raw/fred")
os.makedirs(RAW_DIR, exist_ok=True)

FRED_SERIES = {
    "GDP": "Gross Domestic Product (Billions of Dollars)",
    "UNRATE": "Unemployment Rate (%)",
    "CPIAUCSL": "Consumer Price Index (CPI, 1982-84=100)",
    "FEDFUNDS": "Federal Funds Effective Interest Rate (%)"
}

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

def fetch_series(series_id):
    url = f"{BASE_URL}?series_id={series_id}&api_key={API_KEY}&file_type=json"
    try:
        res = requests.get(url, timeout=20)
        res.raise_for_status()
        data = res.json()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(RAW_DIR, f"{series_id}_{timestamp}.json")

        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)

        logging.info(f"Saved {series_id} → {out_path}")
        return series_id

    except Exception as e:
        logging.error(f"Error fetching {series_id}: {e}")
        return None

def main():
    logging.info("Starting FRED data extraction...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(fetch_series, s) for s in FRED_SERIES.keys()]
        for f in as_completed(futures):
            _ = f.result()

    logging.info("All series fetched successfully.")

if __name__ == "__main__":
    main()
