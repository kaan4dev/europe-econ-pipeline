import os
import json
import logging
from pyspark.sql import SparkSession, Row
from dotenv import load_dotenv

# -----------------------------------------------------------
# 1. Ortam ve log yapılandırması
# -----------------------------------------------------------
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

RAW_DIR = os.path.join(CURRENT_DIR, "data/raw/fred")
PROCESSED_DIR = os.path.join(CURRENT_DIR, "data/processed/fred")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# -----------------------------------------------------------
# 2. Spark oturumu
# -----------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("FRED_Transform_Pipeline")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# -----------------------------------------------------------
# 3. JSON'u açıp flatten eden fonksiyon
# -----------------------------------------------------------
def parse_fred_json(path):
    with open(path, "r") as f:
        data = json.load(f)

    # Dosya adından seriyi çıkar (örneğin GDP_20251029_220531.json → GDP)
    file_name = os.path.basename(path)
    series_id = file_name.split("_")[0]

    observations = data.get("observations", [])
    rows = []

    for obs in observations:
        date = obs.get("date")
        value = obs.get("value")

        if not value or value == ".":
            continue

        try:
            value = float(value)
        except ValueError:
            value = None

        rows.append(Row(series_id=series_id, date=date, value=value))

    return rows


# -----------------------------------------------------------
# 4. Tüm dosyaları dönüştür ve Parquet yaz
# -----------------------------------------------------------
def main():
    all_rows = []
    json_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".json")]

    if not json_files:
        logging.warning("No JSON files found in data/raw/fred/")
        return

    for file in json_files:
        path = os.path.join(RAW_DIR, file)
        logging.info(f"Processing {file}")
        rows = parse_fred_json(path)
        all_rows.extend(rows)

    if not all_rows:
        logging.warning("No valid records found in any JSON file.")
        return

    df = spark.createDataFrame(all_rows)
    df.write.mode("overwrite").parquet(PROCESSED_DIR)
    logging.info(f"✅ Saved processed data to: {PROCESSED_DIR}")
    logging.info("🎯 All FRED datasets transformed successfully.")


if __name__ == "__main__":
    main()
    spark.stop()
