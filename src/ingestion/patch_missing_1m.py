"""
Yahoo Finance 1-Minute Backfill (The Healer)
Fetches missing 1m data for watchlist symbols and patches the PostgreSQL ohlcv_1m table.
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
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
logger = logging.getLogger("stockflow.ingestion.patch1m")

UPSERT_1M_SQL = """
INSERT INTO ohlcv_1m (symbol, asset_class, window_start, open, high, low, close, volume, tick_count)
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

def get_max_1m_date(symbol):
    try:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(window_start) FROM ohlcv_1m WHERE symbol = %s", (symbol,))
                res = cur.fetchone()
                if res and res[0]:
                    return res[0]
    except Exception as e:
        logger.error(f"Error checking max 1m date for {symbol}: {e}")
    return None

def patch_missing():
    symbols_map = {}
    for s in WATCHLIST:
        asset_class = 'crypto' if 'BINANCE' in s else 'stock'
        if s.startswith("BINANCE:"):
            base = s.replace("BINANCE:", "")
            if base.endswith("USDT"):
                symbols_map[s] = (base[:-4] + "-USD", asset_class)
            else:
                symbols_map[s] = (s, asset_class)
        else:
            symbols_map[s] = (s, asset_class)
            
    logger.info("Starting 1-minute data patch process...")
    
    for original_symbol, (yf_symbol, asset_class) in symbols_map.items():
        try:
            max_date = get_max_1m_date(original_symbol)
            
            # Yahoo 1m data is only available for the last 7 days
            seven_days_ago = datetime.now() - timedelta(days=6)
            
            if not max_date or max_date.replace(tzinfo=None) < seven_days_ago:
                start_date = seven_days_ago
            else:
                start_date = max_date.replace(tzinfo=None)
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            
            logger.info(f"Patching {yf_symbol} 1m data from {start_date_str} to now...")
            df = yf.download(yf_symbol, start=start_date_str, interval="1m", progress=False)
            
            if df.empty:
                logger.warning(f"No 1m data found for {yf_symbol} since {start_date_str}")
                continue
                
            df = df.reset_index()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            
            time_col = 'datetime' if 'datetime' in df.columns else 'date'
            if time_col not in df.columns:
                logger.error(f"Could not find time column in {df.columns}")
                continue
                
            df[time_col] = pd.to_datetime(df[time_col])
            
            if max_date:
                df = df[df[time_col] > max_date.replace(tzinfo=df[time_col].dt.tz)]
                
            if df.empty:
                logger.info(f"No NEW 1m data for {yf_symbol}. Already up to date.")
                continue
            
            df_clean = df[[time_col, 'open', 'high', 'low', 'close', 'volume']].copy()
            df_clean.rename(columns={time_col: 'window_start'}, inplace=True)
            df_clean['symbol'] = original_symbol
            df_clean['asset_class'] = asset_class
            df_clean['tick_count'] = 1 # Fake tick count for patched data
            df_clean.dropna(subset=['close'], inplace=True)
            
            df_clean['window_start'] = df_clean['window_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
            records = df_clean[['symbol', 'asset_class', 'window_start', 'open', 'high', 'low', 'close', 'volume', 'tick_count']].values.tolist()
            
            logger.info(f"Upserting {len(records)} PATCHED 1m records into PostgreSQL for {original_symbol}...")
            with get_pg_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, UPSERT_1M_SQL, records)
                conn.commit()
            logger.info(f"Finished patching {original_symbol}")
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error patching {original_symbol}: {e}")

if __name__ == "__main__":
    patch_missing()
