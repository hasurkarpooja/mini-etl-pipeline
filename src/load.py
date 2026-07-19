"""
Load stage.

Reads curated parquet output from the transform stage and loads it into
PostgreSQL, using batched inserts into a staging table followed by an
upsert into the target table (idempotent — safe to re-run for a given day).

Usage:
    python src/load.py --input data/curated/2024-01
"""
import argparse
import logging

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS curated.hourly_zone_fares (
    pickup_location_id INTEGER,
    pickup_hour SMALLINT,
    avg_fare NUMERIC(10, 2),
    avg_duration_min NUMERIC(10, 2),
    trip_count INTEGER,
    PRIMARY KEY (pickup_location_id, pickup_hour)
);
"""

UPSERT_SQL = """
INSERT INTO curated.hourly_zone_fares
    (pickup_location_id, pickup_hour, avg_fare, avg_duration_min, trip_count)
VALUES %s
ON CONFLICT (pickup_location_id, pickup_hour)
DO UPDATE SET
    avg_fare = EXCLUDED.avg_fare,
    avg_duration_min = EXCLUDED.avg_duration_min,
    trip_count = EXCLUDED.trip_count;
"""


def load_hourly_zone_fares(input_dir: str) -> None:
    df = pd.read_parquet(f"{input_dir}/hourly_zone_fares")
    logger.info("Loading %d rows into curated.hourly_zone_fares", len(df))

    records = list(
        df[["PULocationID", "pickup_hour", "avg_fare", "avg_duration_min", "trip_count"]].itertuples(
            index=False, name=None
        )
    )

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")
            cur.execute(CREATE_TABLE_SQL)
            execute_values(cur, UPSERT_SQL, records)
        conn.commit()
        logger.info("Load complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load curated NYC TLC data into Postgres.")
    parser.add_argument("--input", required=True, help="Directory containing curated parquet output")
    args = parser.parse_args()

    load_hourly_zone_fares(args.input)
