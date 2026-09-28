"""
Gold Stream Processor - Pure Python (Pandas + DeltaLake + PostgreSQL)

Reads cleaned trade data from the Silver layer (Delta Lake on S3),
aggregates into 1-minute OHLCV candles, computes technical indicators
(VWAP, tick count), and writes results to PostgreSQL for Dashboard consumption.
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from deltalake import DeltaTable

from src.config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET_NAME,
    S3_REGION,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("stockflow.streaming.gold")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
POLL_INTERVAL_SEC = 30
SILVER_URI = f"s3://{S3_BUCKET_NAME}/silver/trades"

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": S3_REGION
}

# ---------------------------------------------------------------------------
# PostgreSQL Setup
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ohlcv_1m (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(50)   NOT NULL,
    asset_class     VARCHAR(20)   NOT NULL,
    window_start    TIMESTAMP     NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    vwap            DOUBLE PRECISION,
    tick_count       INTEGER       NOT NULL,
    created_at      TIMESTAMP     DEFAULT NOW(),
    UNIQUE (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol ON ohlcv_1m (symbol);
CREATE INDEX IF NOT EXISTS idx_ohlcv_window ON ohlcv_1m (window_start);
CREATE INDEX IF NOT EXISTS idx_ohlcv_asset ON ohlcv_1m (asset_class);
"""

UPSERT_SQL = """
INSERT INTO ohlcv_1m (symbol, asset_class, window_start, open, high, low, close, volume, vwap, tick_count)
VALUES %s
ON CONFLICT (symbol, window_start) DO UPDATE SET
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    close      = EXCLUDED.close,
    volume     = EXCLUDED.volume,
    vwap       = EXCLUDED.vwap,
    tick_count  = EXCLUDED.tick_count,
    created_at = NOW()
"""


def get_pg_connection():
    """Create a PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def init_database():
    """Create tables and indexes if they don't exist."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("PostgreSQL tables initialized successfully.")
    finally:
        conn.close()


def get_last_processed_time() -> Optional[datetime]:
    """Get the latest window_start from PostgreSQL to avoid reprocessing."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(window_start) FROM ohlcv_1m;")
            result = cur.fetchone()
            return result[0] if result and result[0] else None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# OHLCV Aggregation Logic
# ---------------------------------------------------------------------------
def compute_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate tick-level trades into 1-minute OHLCV candles.
    
    Input columns: symbol, price, volume, trade_time, asset_class
    Output columns: symbol, asset_class, window_start, open, high, low, close, volume, vwap, tick_count
    """
    if df.empty:
        return pd.DataFrame()
    
    # Ensure trade_time is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["trade_time"]):
        df["trade_time"] = pd.to_datetime(df["trade_time"])
    
    # Remove timezone info for grouping (PostgreSQL TIMESTAMP without tz)
    if df["trade_time"].dt.tz is not None:
        df["trade_time"] = df["trade_time"].dt.tz_localize(None)
    
    # Floor to 1-minute window
    df["window_start"] = df["trade_time"].dt.floor("1min")
    
    # Calculate VWAP components
    df["pv"] = df["price"] * df["volume"]
    
    # Group by symbol + 1-minute window
    agg = df.groupby(["symbol", "asset_class", "window_start"]).agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("volume", "sum"),
        pv_sum=("pv", "sum"),
        tick_count=("price", "count")
    ).reset_index()
    
    # VWAP = sum(price * volume) / sum(volume)
    agg["vwap"] = agg["pv_sum"] / agg["volume"]
    agg["vwap"] = agg["vwap"].round(6)
    agg.drop(columns=["pv_sum"], inplace=True)
    
    return agg


# ---------------------------------------------------------------------------
# Write to PostgreSQL
# ---------------------------------------------------------------------------
def write_to_postgres(df_ohlcv: pd.DataFrame) -> int:
    """Upsert OHLCV candles into PostgreSQL. Returns number of rows written."""
    if df_ohlcv.empty:
        return 0
    
    # Prepare tuples for execute_values
    records = [
        (
            row["symbol"],
            row["asset_class"],
            row["window_start"],
            round(row["open"], 6),
            round(row["high"], 6),
            round(row["low"], 6),
            round(row["close"], 6),
            round(row["volume"], 6),
            row["vwap"],
            row["tick_count"]
        )
        for _, row in df_ohlcv.iterrows()
    ]
    
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, records)
        conn.commit()
        return len(records)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def run_gold_stream():
    """Main processing loop: Silver (Delta Lake) -> OHLCV -> PostgreSQL."""
    
    # Initialize database tables
    init_database()
    
    running = True
    
    def _signal_handler(signum: int, frame: Any) -> None:
        nonlocal running
        logger.info("Received stop signal, shutting down gracefully...")
        running = False
    
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    
    logger.info("Gold Stream started. Polling Silver Delta Lake for new data...")
    
    last_version = -1
    
    while running:
        try:
            # 1. Read from Silver Delta Lake
            dt = DeltaTable(SILVER_URI, storage_options=STORAGE_OPTIONS)
            current_version = dt.version()
            
            if current_version == last_version:
                logger.debug(f"No new data (version={current_version}). Waiting...")
                for _ in range(POLL_INTERVAL_SEC):
                    if not running:
                        break
                    time.sleep(1)
                continue
            
            logger.info(f"New Delta version detected: {last_version} -> {current_version}")
            
            # Read entire Silver table
            df = dt.to_pandas(
                columns=["symbol", "price", "volume", "trade_time", "asset_class"]
            )
            
            if df.empty:
                logger.warning("Silver Delta table is empty.")
                last_version = current_version
                continue
            
            logger.info(f"Read {len(df)} records from Silver Delta Lake.")
            
            # 2. Only process data newer than what we already have in PostgreSQL
            last_processed = get_last_processed_time()
            if last_processed:
                # Normalize trade_time: strip tz, cast to datetime64[ns]
                if df["trade_time"].dt.tz is not None:
                    df["trade_time"] = df["trade_time"].dt.tz_localize(None)
                df["trade_time"] = df["trade_time"].astype("datetime64[ns]")
                # Convert cutoff to numpy datetime64 for guaranteed compatibility
                import numpy as np
                cutoff = np.datetime64(str(last_processed))
                df = df[df["trade_time"].values > cutoff]
                logger.info(f"After filtering already-processed: {len(df)} new records.")
            
            if df.empty:
                logger.info("No new records to process.")
                last_version = current_version
                for _ in range(POLL_INTERVAL_SEC):
                    if not running:
                        break
                    time.sleep(1)
                continue
            
            # 3. Compute OHLCV candles
            df_ohlcv = compute_ohlcv(df)
            
            if df_ohlcv.empty:
                last_version = current_version
                continue
            
            # 4. Write to PostgreSQL
            rows_written = write_to_postgres(df_ohlcv)
            logger.info(
                f"Gold Layer updated | "
                f"Candles: {rows_written} | "
                f"Symbols: {df_ohlcv['symbol'].nunique()} | "
                f"Time range: {df_ohlcv['window_start'].min()} -> {df_ohlcv['window_start'].max()}"
            )
            
            last_version = current_version
            
        except Exception as e:
            logger.error(f"Error in Gold Stream: {e}")
        
        # Sleep before next poll
        for _ in range(POLL_INTERVAL_SEC):
            if not running:
                break
            time.sleep(1)
    
    logger.info("Gold Stream stopped.")


if __name__ == "__main__":
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        logger.error("AWS credentials missing in .env file!")
        sys.exit(1)
    
    if not POSTGRES_HOST or not POSTGRES_PASSWORD:
        logger.error("PostgreSQL credentials missing in .env file!")
        sys.exit(1)
    
    run_gold_stream()
