# NYC Yellow Taxi ETL Pipeline

An end-to-end batch ETL pipeline built with **PySpark**, orchestrated with **Apache Airflow**, loading curated
data into **PostgreSQL**. Built to demonstrate a production-style batch data engineering workflow: extract
raw trip data → clean & transform with Spark → load aggregated/curated tables into a warehouse, on a
scheduled, monitored, and reproducible pipeline.

## Architecture

```
                ┌──────────────┐      ┌───────────────────┐      ┌──────────────────┐
   NYC TLC      │   Extract    │      │     Transform      │      │       Load        │
   Trip Data ──▶│  (download   │─────▶│  (PySpark: clean,  │─────▶│  (PostgreSQL:      │
   (public,     │  raw parquet)│      │  dedupe, enrich,   │      │  fact + dim tables)│
   monthly)     └──────────────┘      │  aggregate)         │      └──────────────────┘
                                       └───────────────────┘
                        ▲                       ▲                          ▲
                        └───────────────────────┴──────────────────────────┘
                                    Orchestrated by Airflow DAG
                              (schedule, retries, logging, alerting)
```

## Why this project

This mirrors the batch ETL patterns used in production data engineering roles: pulling large public
datasets, transforming them at scale with Spark, and landing clean, query-ready tables in a warehouse —
with orchestration, retry logic, and version-controlled code, rather than a one-off notebook.

## Tech stack

| Layer | Tool |
|---|---|
| Processing | PySpark |
| Orchestration | Apache Airflow |
| Storage (raw) | Parquet (local / S3-compatible) |
| Storage (curated) | PostgreSQL |
| CI | GitHub Actions (lint + unit tests on push) |
| Language | Python 3.10+ |

## Project structure

```
nyc-taxi-etl-pipeline/
├── dags/
│   └── taxi_etl_dag.py          # Airflow DAG: extract -> transform -> load
├── src/
│   ├── extract.py               # Downloads raw monthly trip data
│   ├── transform.py             # PySpark cleaning + aggregation logic
│   ├── load.py                  # Writes curated tables to Postgres
│   └── config.py                # Paths, DB connection settings
├── tests/
│   └── test_transform.py        # Unit tests for transform logic
├── data/sample/                 # Small sample file for local testing
├── .github/workflows/ci.yml     # Lint + test on every push
├── docker-compose.yml           # Local Postgres for dev/testing
├── requirements.txt
└── README.md
```

## Pipeline stages

1. **Extract** (`src/extract.py`) — downloads a monthly NYC Yellow Taxi trip parquet file from the
   [NYC TLC public dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) into raw storage.
2. **Transform** (`src/transform.py`) — PySpark job that:
   - Drops malformed/duplicate records and invalid fares/timestamps
   - Derives trip duration, pickup hour, and weekday
   - Aggregates: average fare and trip duration by pickup zone and hour
3. **Load** (`src/load.py`) — writes the curated fact table and a daily aggregate table into PostgreSQL
   using batched upserts.
4. **Orchestration** (`dags/taxi_etl_dag.py`) — an Airflow DAG chains the three stages, runs daily,
   retries on failure, and emails/logs on error.

## Running locally

```bash
# 1. Start local Postgres
docker-compose up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline manually (without Airflow, for local testing)
python src/extract.py --month 2024-01
python src/transform.py --input data/raw/2024-01.parquet --output data/curated/2024-01
python src/load.py --input data/curated/2024-01

# 4. Or run under Airflow
airflow standalone
# then trigger the `nyc_taxi_etl` DAG from the UI
```

## Running tests

```bash
pytest tests/
```

## Sample output table (curated.hourly_zone_fares)

| pickup_zone | pickup_hour | avg_fare | avg_duration_min | trip_count |
|---|---|---|---|---|
| Midtown East | 8 | 14.22 | 12.4 | 1,204 |
| JFK Airport | 17 | 52.10 | 38.9 | 389 |

## Next steps / possible extensions

- Swap local Postgres for Snowflake/Synapse to mirror cloud-warehouse patterns
- Add data quality checks with Great Expectations
- Partition curated storage by month for incremental loads
