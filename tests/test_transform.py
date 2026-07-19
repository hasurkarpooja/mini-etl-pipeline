"""
Unit tests for the transform stage. Uses a local Spark session with a tiny
in-memory dataset so tests run fast and need no external data.
"""
import sys
import os
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from transform import clean_trips, aggregate_hourly_zone_fares  # noqa: E402




@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("test").master("local[2]").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def sample_df(spark):
    data = [
        (1, datetime(2024, 1, 1, 8, 0), datetime(2024, 1, 1, 8, 15), 12.50, 100),
        (2, datetime(2024, 1, 1, 8, 5), datetime(2024, 1, 1, 8, 20), 600.00, 100),  # invalid fare
        (3, datetime(2024, 1, 1, 17, 0), datetime(2024, 1, 1, 17, 3), 8.00, 200),   # too short a trip
    ]
    columns = [
        "trip_id",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "PULocationID",
    ]
    return spark.createDataFrame(data, columns)


def test_clean_trips_filters_invalid_fares(sample_df):
    cleaned = clean_trips(sample_df)
    fares = [row.fare_amount for row in cleaned.collect()]
    assert 600.00 not in fares


def test_clean_trips_adds_derived_columns(sample_df):
    cleaned = clean_trips(sample_df)
    assert "trip_duration_min" in cleaned.columns
    assert "pickup_hour" in cleaned.columns
    assert "pickup_weekday" in cleaned.columns


def test_aggregate_hourly_zone_fares_groups_correctly(sample_df):
    cleaned = clean_trips(sample_df)
    agg = aggregate_hourly_zone_fares(cleaned)
    result = agg.collect()
    assert len(result) >= 1
    for row in result:
        assert row.trip_count >= 1
