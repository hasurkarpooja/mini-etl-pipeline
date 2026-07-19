"""
Transform stage.

Reads raw trip-record parquet, cleans and enriches it with PySpark, and
writes both a cleaned fact table and an hourly zone-level aggregate to the
curated storage zone.

Usage:
    python src/transform.py --input data/raw/2024-01.parquet --output data/curated/2024-01
"""
import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import MAX_FARE, MAX_TRIP_DURATION_MIN, MIN_FARE, MIN_TRIP_DURATION_MIN

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "nyc_taxi_transform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def clean_trips(df: DataFrame) -> DataFrame:
    """Drop malformed records, dedupe, and derive trip-level fields."""
    df = df.dropDuplicates()

    df = df.withColumn(
        "trip_duration_min",
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60,
    )

    df = df.filter(
        (F.col("fare_amount") >= MIN_FARE)
        & (F.col("fare_amount") <= MAX_FARE)
        & (F.col("trip_duration_min") >= MIN_TRIP_DURATION_MIN)
        & (F.col("trip_duration_min") <= MAX_TRIP_DURATION_MIN)
        & F.col("PULocationID").isNotNull()
    )

    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("pickup_weekday", F.date_format("tpep_pickup_datetime", "EEEE"))

    return df


def aggregate_hourly_zone_fares(df: DataFrame) -> DataFrame:
    """Average fare/duration and trip count per pickup zone and hour."""
    return (
        df.groupBy("PULocationID", "pickup_hour")
        .agg(
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
            F.count("*").alias("trip_count"),
        )
        .orderBy("PULocationID", "pickup_hour")
    )


def run(input_path: str, output_path: str) -> None:
    spark = get_spark_session()
    logger.info("Reading raw data from %s", input_path)
    raw_df = spark.read.parquet(input_path)

    cleaned_df = clean_trips(raw_df)
    logger.info("Cleaned row count: %d", cleaned_df.count())

    hourly_agg_df = aggregate_hourly_zone_fares(cleaned_df)

    cleaned_df.write.mode("overwrite").parquet(f"{output_path}/trips_cleaned")
    hourly_agg_df.write.mode("overwrite").parquet(f"{output_path}/hourly_zone_fares")

    logger.info("Transform complete. Output written to %s", output_path)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and aggregate NYC TLC trip data.")
    parser.add_argument("--input", required=True, help="Path to raw parquet file")
    parser.add_argument("--output", required=True, help="Directory for curated output")
    args = parser.parse_args()

    run(args.input, args.output)
