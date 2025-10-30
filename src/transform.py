import os
import json
import logging
from pyspark.sql import SparkSession, Row
from dotenv import load_dotenv

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
MODELED_DIR = os.path.join(CURRENT_DIR, "data/modeled/fred")
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(MODELED_DIR, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("FRED_Transform_Pipeline")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

def parse_fred_json(path):
    with open(path, "r") as f:
        data = json.load(f)

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

def transform_raw_to_processed():
    all_rows = []
    json_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".json")]

    if not json_files:
        logging.warning("No JSON files found in data/raw/fred/")
        return None

    for file in json_files:
        path = os.path.join(RAW_DIR, file)
        logging.info(f"Processing {file}")
        rows = parse_fred_json(path)
        all_rows.extend(rows)

    if not all_rows:
        logging.warning("No valid records found in any JSON file.")
        return None

    df = spark.createDataFrame(all_rows)
    df.write.mode("overwrite").parquet(PROCESSED_DIR)
    logging.info(f"Saved processed data to: {PROCESSED_DIR}")
    return df

def pivot_processed_to_modeled(df):
    logging.info("Pivoting processed data into wide format...")

    pivoted = (
        df.groupBy("date")
          .pivot("series_id")
          .avg("value")
          .orderBy("date")
    )

    out_path = os.path.join(MODELED_DIR, "fred_pivoted.parquet")
    pivoted.write.mode("overwrite").parquet(out_path)

    logging.info(f"Pivoted dataset saved to: {out_path}")
    logging.info("All FRED series transformed and merged successfully.")

if __name__ == "__main__":
    df = transform_raw_to_processed()
    if df is not None:
        pivot_processed_to_modeled(df)
    spark.stop()
