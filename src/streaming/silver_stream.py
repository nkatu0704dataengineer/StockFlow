"""
Silver Stream Processor - Pure Python (Pandas + DeltaLake)

Reads raw Parquet trade data from the Bronze layer in AWS S3,
performs data quality checks, deduplication, and enrichment,
then writes the cleaned data to the Silver layer in Delta Lake format.
"""

import json
import logging
import os
import signal
import sys
import time
from typing import Any, List, Set

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from deltalake import write_deltalake

from src.config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET_NAME,
    S3_REGION
)

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("stockflow.streaming.silver")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
POLL_INTERVAL_SEC = 10
CHECKPOINT_FILE = "silver_checkpoint.json"
BRONZE_PREFIX = "bronze/trades/"
SILVER_URI = f"s3://{S3_BUCKET_NAME}/silver/trades"

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": S3_REGION
}

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION
)

# ---------------------------------------------------------------------------
# Checkpoint Management
# ---------------------------------------------------------------------------
def load_checkpoint() -> Set[str]:
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("processed_files", []))
        except Exception as e:
            logger.error(f"Error reading checkpoint: {e}")
    return set()

def save_checkpoint(processed_files: Set[str]) -> None:
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump({"processed_files": list(processed_files)}, f)
    except Exception as e:
        logger.error(f"Error saving checkpoint: {e}")

# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------
def get_new_bronze_files(processed_files: Set[str]) -> List[str]:
    """Poll S3 for new parquet files in the Bronze layer."""
    new_files = []
    try:
        # Use paginator to handle >1000 objects if needed
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=BRONZE_PREFIX)
        
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if key.endswith(".parquet") and key not in processed_files:
                        new_files.append(key)
                        
    except ClientError as e:
        logger.error(f"Failed to list S3 objects: {e}")
        
    return sorted(new_files)

def process_files(file_keys: List[str]) -> bool:
    """Process a batch of new bronze files and write to silver."""
    if not file_keys:
        return True
        
    logger.info(f"Processing micro-batch of {len(file_keys)} new files...")
    
    # 1. Read all new Parquet files into a single DataFrame
    dfs = []
    
    s3fs_options = {
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
        "client_kwargs": {"region_name": S3_REGION}
    }
    
    try:
        for key in file_keys:
            s3_uri = f"s3://{S3_BUCKET_NAME}/{key}"
            dfs.append(pd.read_parquet(s3_uri, storage_options=s3fs_options))
        df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        logger.error(f"Error reading bronze files: {e}")
        return False
        
    initial_count = len(df)
    
    # 2. Data Quality (Filter Anomalies)
    df = df[(df["price"] > 0) & (df["volume"] > 0)]
    
    # 3. Enrichment
    # Convert timestamp (ms) to UTC datetime
    df["trade_time"] = pd.to_datetime(df["source_timestamp"], unit="ms", utc=True)
    # Asset class logic
    df["asset_class"] = df["symbol"].apply(lambda x: "Crypto" if "BINANCE" in x else "Stock")
    
    # 4. Deduplication
    df = df.drop_duplicates(subset=["event_id"])
    
    final_count = len(df)
    logger.info(f"Filtered {initial_count - final_count} invalid/duplicate records. Total valid: {final_count}")
    
    if final_count == 0:
        return True
        
    # 5. Write to Silver (Delta Lake)
    import pyarrow as pa
    try:
        logger.info(f"Writing to Delta Lake at {SILVER_URI} ...")
        
        # Explicit schema to avoid "Null" type inference errors
        schema = pa.schema([
            ("event_id", pa.string()),
            ("symbol", pa.string()),
            ("price", pa.float64()),
            ("volume", pa.float64()),
            ("source_timestamp", pa.int64()),
            ("ingestion_timestamp", pa.int64()),
            ("year", pa.string()),
            ("month", pa.string()),
            ("day", pa.string()),
            ("trade_time", pa.timestamp('ns', tz='UTC')),
            ("asset_class", pa.string()),
        ])
        
        # Ensure year, month, day are standard strings (not category or int)
        df["year"] = df["year"].astype(str)
        df["month"] = df["month"].astype(str)
        df["day"] = df["day"].astype(str)
        
        # Drop any leftover __index_level_0__ columns
        if "__index_level_0__" in df.columns:
            df = df.drop(columns=["__index_level_0__"])
            
        # Reorder columns to match schema
        df = df[["event_id", "symbol", "price", "volume", "source_timestamp", "ingestion_timestamp", "year", "month", "day", "trade_time", "asset_class"]]
        
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        
        write_deltalake(
            SILVER_URI,
            table,
            mode="append",
            partition_by=["asset_class", "year", "month", "day"],
            storage_options=STORAGE_OPTIONS
        )
        logger.info(f"Successfully wrote {final_count} records to Silver Delta Lake.")
        return True
    except Exception as e:
        logger.error(f"Error writing to Delta Lake: {e}")
        return False

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def run_silver_stream():
    processed_files = load_checkpoint()
    logger.info(f"Silver Stream started. Loaded {len(processed_files)} processed files from checkpoint.")
    
    running = True
    
    def _signal_handler(signum: int, frame: Any) -> None:
        logger.info("Received stop signal, shutting down gracefully...")
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    
    while running:
        new_files = get_new_bronze_files(processed_files)
        
        if new_files:
            success = process_files(new_files)
            if success:
                processed_files.update(new_files)
                save_checkpoint(processed_files)
        else:
            logger.debug("No new files found. Waiting...")
            
        # Sleep before next poll
        for _ in range(POLL_INTERVAL_SEC):
            if not running:
                break
            time.sleep(1)

    logger.info("Silver Stream stopped.")

if __name__ == "__main__":
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        logger.error("AWS credentials missing in .env file!")
        sys.exit(1)
        
    run_silver_stream()
