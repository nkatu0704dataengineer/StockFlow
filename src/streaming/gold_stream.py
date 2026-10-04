"""
Gold Stream Processor - PySpark Structured Streaming

Reads raw trade data from the Silver layer (Delta Lake on S3),
aggregates into 1-minute OHLCV candles using PySpark Stateful Streaming
with Watermarking, and writes results to PostgreSQL using foreachBatch.
This architecture prevents Out-Of-Memory (OOM) issues and scales infinitely.
"""

import os
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin;' + os.environ.get('PATH', '')

import logging
import sys
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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
# PySpark requires s3a:// for AWS S3
SILVER_URI = f"s3a://{S3_BUCKET_NAME}/silver/trades"
CHECKPOINT_DIR = f"s3a://{S3_BUCKET_NAME}/checkpoints/gold_stream"

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
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def init_database():
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("PostgreSQL tables initialized successfully.")
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# ForeachBatch Writer
# ---------------------------------------------------------------------------
def write_to_postgres_foreach(batch_df, batch_id):
    """
    This function is called by Spark for each micro-batch.
    Since the batch contains only the aggregated 1-minute candles,
    it is extremely small and safe to convert to Pandas for quick UPSERT.
    """
    pdf = batch_df.toPandas()
    if pdf.empty:
        return
    
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
            round(row["vwap"], 6) if row["vwap"] else 0.0,
            row["tick_count"]
        )
        for _, row in pdf.iterrows()
    ]
    
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, records)
        conn.commit()
        logger.info(f"Batch {batch_id}: Upserted {len(records)} candles to PostgreSQL (Symbols: {pdf['symbol'].nunique()}).")
    except Exception as e:
        logger.error(f"Batch {batch_id}: Error writing to PostgreSQL: {e}")
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Main Spark Application
# ---------------------------------------------------------------------------
def run_gold_stream():
    init_database()
    
    # Initialize Spark Session configured for Delta and S3
    logger.info("Initializing PySpark Session...")
    spark = (SparkSession.builder
        .appName("GoldStream_PySpark")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Add packages for Delta and AWS S3
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4")
        # AWS Credentials
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{S3_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Performance tuning for streaming
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    # Supress noisy Spark logs
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Reading Silver Delta Lake stream...")
    # 1. Read Stream from Silver Delta Lake
    df = spark.readStream.format("delta").load(SILVER_URI)
    
    # Convert trade_time to timestamp (Delta Lake might store it as string/timestamp)
    df = df.withColumn("trade_time_ts", F.to_timestamp(F.col("trade_time")))
    
    # 2. Stateful Aggregation (1-minute candles)
    # Using withWatermark allows Spark to drop old data from memory, preventing OOM
    agg_df = (df
        .withWatermark("trade_time_ts", "5 minutes")
        .groupBy(
            F.window(F.col("trade_time_ts"), "1 minute"),
            F.col("symbol"),
            F.col("asset_class")
        )
        .agg(
            # Open: First price (min trade_time)
            F.min(F.struct(F.col("trade_time_ts"), F.col("price"))).getField("price").alias("open"),
            # High: Max price
            F.max(F.col("price")).alias("high"),
            # Low: Min price
            F.min(F.col("price")).alias("low"),
            # Close: Last price (max trade_time)
            F.max(F.struct(F.col("trade_time_ts"), F.col("price"))).getField("price").alias("close"),
            # Volume: Sum of volumes
            F.sum(F.col("volume")).alias("volume"),
            # VWAP = Sum(Price * Volume) / Sum(Volume)
            (F.sum(F.col("price") * F.col("volume")) / F.sum(F.col("volume"))).alias("vwap"),
            # Tick count
            F.count("*").alias("tick_count")
        )
        # Extract the start time of the window as the window_start column
        .withColumn("window_start", F.col("window.start"))
        .drop("window")
    )
    
    # 3. Write Stream to PostgreSQL using foreachBatch
    logger.info("Starting Streaming Query to PostgreSQL...")
    query = (agg_df.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres_foreach)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        logger.error("AWS credentials missing in .env file!")
        sys.exit(1)
        
    if not POSTGRES_HOST or not POSTGRES_PASSWORD:
        logger.error("PostgreSQL credentials missing in .env file!")
        sys.exit(1)
        
    run_gold_stream()
