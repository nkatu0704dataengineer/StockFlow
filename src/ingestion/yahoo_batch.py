"""
Yahoo Finance Batch Ingestion Script
Fetches daily historical data for watchlist symbols and stores directly to PostgreSQL.
Saves raw data to S3 Bronze layer for backup.
Supports Incremental Loading.
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values

from src.config.settings import (
    WATCHLIST,
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
logger = logging.getLogger("stockflow.ingestion.yahoo")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ohlcv_daily (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(50)   NOT NULL,
    window_start    TIMESTAMP     NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMP     DEFAULT NOW(),
    UNIQUE (symbol, window_start)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol ON ohlcv_daily (symbol);
CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_window ON ohlcv_daily (window_start);
"""

UPSERT_SQL = """
INSERT INTO ohlcv_daily (symbol, window_start, open, high, low, close, volume)
VALUES %s
ON CONFLICT (symbol, window_start) DO UPDATE SET
    open       = EXCLUDED.open,
    high       = EXCLUDED.high,
    low        = EXCLUDED.low,
    close      = EXCLUDED.close,
    volume     = EXCLUDED.volume,
    created_at = NOW()
"""

def get_pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def get_max_date(symbol):
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(window_start) FROM ohlcv_daily WHERE symbol = %s", (symbol,))
                res = cur.fetchone()
                if res and res[0]:
                    return res[0]
    except Exception as e:
        logger.error(f"Error checking max date for {symbol}: {e}")
    return None

def fetch_and_process():
    # Convert Finnhub symbols to Yahoo symbols (e.g. BINANCE:BTCUSDT -> BTC-USD)
    symbols_map = {}
    for s in WATCHLIST:
        if s.startswith("BINANCE:"):
            base = s.replace("BINANCE:", "")
            if base.endswith("USDT"):
                symbols_map[s] = base[:-4] + "-USD"
            else:
                symbols_map[s] = s
        else:
            symbols_map[s] = s
            
    logger.info(f"Fetching data for mapped symbols: {symbols_map}")
    
    # Init postgres table
    logger.info("Initializing PostgreSQL table ohlcv_daily...")
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
    
    storage_options = {
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
        "client_kwargs": {"region_name": S3_REGION}
    }
    

    for original_symbol, yf_symbol in symbols_map.items():
        try:
            max_date = get_max_date(original_symbol)
            if max_date:
                # Add 1 day to max_date to avoid re-downloading the exact same day unnecessarily,
                # though yfinance might require max_date to be today to pull today's partial data.
                # Let's just use max_date to ensure we get updates for the latest day if it wasn't closed.
                start_date_str = max_date.strftime("%Y-%m-%d")
                logger.info(f"Incremental load for {yf_symbol} from {start_date_str}...")
                df = yf.download(yf_symbol, start=start_date_str, interval="1d", progress=False)
            else:
                logger.info(f"Full load for {yf_symbol} (3 years)...")
                df = yf.download(yf_symbol, period="3y", interval="1d", progress=False)
            
            if df.empty:
                logger.warning(f"No data found for {yf_symbol}")
                continue
                
            df = df.reset_index()
            # Standardize columns to lowercase
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            
            df['date'] = pd.to_datetime(df['date'])
            
            df_clean = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_clean.rename(columns={'date': 'window_start'}, inplace=True)
            df_clean['symbol'] = original_symbol
            df_clean.dropna(subset=['close'], inplace=True)
            
            # Save raw to S3 Bronze
            s3_path = f"s3://{S3_BUCKET_NAME}/Raw_yh/trades/year={datetime.now().year}/month={datetime.now().month:02d}/day={datetime.now().day:02d}/{original_symbol.replace(':', '_')}_historical.parquet"
            logger.info(f"Saving {len(df_clean)} raw records to {s3_path}")
            df_clean.to_parquet(s3_path, index=False, storage_options=storage_options)
            
            # Prepare for PostgreSQL
            df_clean['window_start'] = df_clean['window_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
            records = df_clean[['symbol', 'window_start', 'open', 'high', 'low', 'close', 'volume']].values.tolist()
            
            logger.info(f"Upserting {len(records)} records into PostgreSQL for {original_symbol}...")
            with get_pg_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, UPSERT_SQL, records)
                conn.commit()
            logger.info(f"Finished processing {original_symbol}")
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error processing {original_symbol}: {e}")

if __name__ == "__main__":
    fetch_and_process()
