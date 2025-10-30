# Europe Economic Indicators ETL Pipeline

This project implements a full ETL pipeline that collects, transforms, and stores macroeconomic indicators from the **FRED (Federal Reserve Economic Data)** API.  
It demonstrates modular data engineering using **Python, Spark, and Parquet**, designed for scalability and integration with cloud data lakes (Azure Data Lake Gen2).

---

## Overview

The pipeline extracts key European economic indicators such as:
- **GDP** (Gross Domestic Product)
- **UNRATE** (Unemployment Rate)
- **CPIAUCSL** (Consumer Price Index)
- **FEDFUNDS** (Federal Funds Rate)

Each indicator is stored as a time series dataset, transformed into structured Parquet format for efficient analytics.

---

## Pipeline Architecture

**Extract → Transform → Load (ETL)**

1. **Extract**  
   - Pulls JSON data from the [FRED API](https://fred.stlouisfed.org/docs/api/fred/) for multiple indicators.  
   - Raw files are saved in `data/raw/fred`.

2. **Transform**  
   - Spark parses and normalizes each dataset.  
   - Filters invalid values (`.`), casts data types, and enriches time fields (`year`, `quarter`).  
   - Saves transformed data to `data/processed/fred/` as Parquet.

3. **Load (Planned)**  
   - Designed to upload processed and modeled layers to **Azure Data Lake Gen2** for cloud storage and further analytics.

---

## Tech Stack

| Layer | Tools / Libraries |
|-------|-------------------|
| Language | Python 3.12 |
| Data Processing | Apache Spark |
| Data Format | JSON → Parquet |
| Cloud Integration | Azure Data Lake (planned) |
| Logging & Config | `logging`, `dotenv` |
| Environment | macOS + venv |

---

##  Project Structure

```
europe-econ-pipeline/
├── data/
│   ├── raw/
│   │   └── fred/
│   │       ├── GDP_20251029_202938.json
│   │       ├── UNRATE_20251029_202938.json
│   │       └── ...
│   ├── processed/
│   │   └── fred/
│   │       ├── gdp.parquet
│   │       ├── unrate.parquet
│   │       └── ...
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   └── utils/
│       └── io_datalake.py
│
├── .env
├── .gitignore
└── README.md
```

---

## Example Transformation (GDP)

**Input JSON (FRED API):**
```json
{
  "observations": [
    {"date": "2020-01-01", "value": "21751.238"},
    {"date": "2020-04-01", "value": "19958.291"}
  ],
  "units": "lin"
}
```

**Output Parquet Schema:**
| date | year | quarter | value | units | indicator_id |
|------|------|----------|--------|--------|---------------|
| 2020-01-01 | 2020 | 1 | 21751.238 | lin | GDP |
| 2020-04-01 | 2020 | 2 | 19958.291 | lin | GDP |

---

## Key Learnings

- Handling nested JSON from APIs with Spark.  
- Designing directory layers (`raw → processed → modeled`).  
- Creating scalable ETL scripts with reusable modules.  
- Preparing data pipelines for cloud migration.
