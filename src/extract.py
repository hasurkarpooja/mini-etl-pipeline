"""
Extract stage.

Downloads a monthly NYC Yellow Taxi trip-record parquet file from the public
TLC dataset and lands it, unmodified, into the raw storage zone.

Usage:
    python src/extract.py --month 2024-01
"""
import argparse
import logging
import os

import requests

from config import NYC_TLC_BASE_URL, RAW_DATA_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def download_month(month: str, dest_dir: str = RAW_DATA_DIR) -> str:
    """Download one month of trip data. `month` format: YYYY-MM."""
    os.makedirs(dest_dir, exist_ok=True)
    url = NYC_TLC_BASE_URL.format(month=month)
    dest_path = os.path.join(dest_dir, f"{month}.parquet")

    if os.path.exists(dest_path):
        logger.info("Raw file for %s already exists, skipping download.", month)
        return dest_path

    logger.info("Downloading %s -> %s", url, dest_path)
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info("Download complete: %s (%.1f MB)", dest_path, os.path.getsize(dest_path) / 1e6)
    return dest_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract NYC TLC trip data for a given month.")
    parser.add_argument("--month", required=True, help="Month to extract, format YYYY-MM")
    args = parser.parse_args()

    download_month(args.month)
