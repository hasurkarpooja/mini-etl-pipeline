"""
Central configuration for the pipeline.
All values can be overridden via environment variables so the same code
runs locally, in CI, and under Airflow without changes.
"""
import os

# --- Storage paths -----------------------------------------------------
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "data/raw")
CURATED_DATA_DIR = os.getenv("CURATED_DATA_DIR", "data/curated")

# --- Source data ---------------------------------------------------------
NYC_TLC_BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
)

# --- Postgres connection --------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "taxi_warehouse"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

# --- Data quality thresholds ---------------------------------------------
MIN_FARE = 0.0
MAX_FARE = 500.0
MIN_TRIP_DURATION_MIN = 1
MAX_TRIP_DURATION_MIN = 180
