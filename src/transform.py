import os
import json
import logging
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, year, quarter
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

spark = (
    SparkSession.builder
    .appName("FRED_Transform_Pipeline")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

CURRENT_DIR = os.getcwd()
while not CURRENT_DIR.endswith("europe-econ-pipeline") and CURRENT_DIR != "/":
    CURRENT_DIR = os.path.dirname(CURRENT_DIR)
os.chdir(CURRENT_DIR)

RAW_DIR = os.path.join(CURRENT_DIR, "data/raw/fred")
PROCESSED_DIR = os.path.join(CURRENT_DIR, "data/processed/fred")
os.makedirs(PROCESSED_DIR, exist_ok=True)

def transform_fred_json(path, indicator_id):
    with open(path, "r") as f:
        data = json.load(f)

    observations = data.get("observations", [])
    units = data.get("units", "unknown")

    rows = []
    for obs in observations:
        value = obs.get("value")
        date = obs.get("date")
        if not value or value == ".":
            continue
        try:
            value = float(value)
        except ValueError:
            continue
        rows.append(Row(date=date, value=value, units=units, indicator_id=indicator_id))

    df = spark.createDataFrame(rows)

    df = (
        df.withColumn("date", col("date").cast("date"))
          .withColumn("year", year(col("date")))
          .withColumn("quarter", quarter(col("date")))
    )
    return df

def transform_all_fred():
    for filename in os.listdir(RAW_DIR):
        if not filename.endswith(".json"):
            continue

        indicator_id = filename.split("_")[0].upper()
        input_path = os.path.join(RAW_DIR, filename)
        out_name = indicator_id.lower() + ".parquet"
        out_path = os.path.join(PROCESSED_DIR, out_name)

        df = transform_fred_json(input_path, indicator_id)
        df.write.mode("overwrite").parquet(out_path)

        logging.info(f"Transformed {indicator_id} → saved to {out_path}")

if __name__ == "__main__":
    transform_all_fred()
    logging.info("🎯 All FRED datasets transformed successfully.")
