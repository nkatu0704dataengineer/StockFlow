"""
Lightweight Python Scheduler
Runs daily batch jobs and real-time gap healers automatically.
"""

import schedule
import time
import logging
import subprocess
import sys
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
logger = logging.getLogger("stockflow.scheduler")

def run_yahoo_batch():
    logger.info("=== STARTING DAILY YAHOO BATCH ===")
    try:
        subprocess.run([sys.executable, "-m", "src.ingestion.yahoo_batch"], check=True)
        logger.info("=== YAHOO BATCH COMPLETED SUCCESSFULLY ===")
    except Exception as e:
        logger.error(f"Yahoo Batch failed: {e}")

def run_patch_1m():
    logger.info("=== STARTING 1M HEALER PATCH ===")
    try:
        subprocess.run([sys.executable, "-m", "src.ingestion.patch_missing_1m"], check=True)
        logger.info("=== 1M HEALER PATCH COMPLETED SUCCESSFULLY ===")
    except Exception as e:
        logger.error(f"1M Patch failed: {e}")

if __name__ == "__main__":
    logger.info("StockFlow Scheduler Started. Press Ctrl+C to exit.")
    
    # Run patching every 4 hours to heal any temporary disconnections
    schedule.every(4).hours.do(run_patch_1m)
    
    # Run daily batch at 5:00 AM (after US market closes and processes)
    schedule.every().day.at("05:00").do(run_yahoo_batch)
    
    # Immediately run them once on startup so we don't have to wait
    logger.info("Running initial startup jobs...")
    run_patch_1m()
    run_yahoo_batch()
    
    while True:
        schedule.run_pending()
        time.sleep(60)
