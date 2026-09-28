"""
Bronze Stream Consumer  -  Kafka -> S3 (Parquet / Delta-ready)

Consumes normalized TradeMessage records from the
``finnhub_realtime_trades`` Kafka topic and writes them to AWS S3
in partitioned Parquet micro-batches.

Architecture
------------
  Kafka topic  --(consume)-->  In-memory buffer  --(flush)-->  S3 Parquet
                                  (every N records or T seconds)

Partitioning strategy on S3::

    s3://<bucket>/bronze/trades/year=2026/month=09/day=27/
        batch_<timestamp>_<uuid>.parquet

Usage
-----
    python -m src.streaming.bronze_stream

The consumer runs indefinitely until interrupted (Ctrl+C).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRADES,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    S3_BUCKET_NAME,
    S3_REGION,
)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stockflow.streaming.bronze")


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
CONSUMER_GROUP_ID = "stockflow-bronze-consumer"

# Flush thresholds: whichever fires first triggers a write to S3
BATCH_SIZE = 5_000           # Max records per micro-batch
FLUSH_INTERVAL_SEC = 30      # Max seconds between flushes

# S3 path prefix
S3_BRONZE_PREFIX = "bronze/trades"


# ---------------------------------------------------------------------------
# S3 Writer
# ---------------------------------------------------------------------------
class S3ParquetWriter:
    """
    Writes pandas DataFrames to S3 as Parquet files.

    Each file is stored with a time-based partition path::

        s3://<bucket>/bronze/trades/year=YYYY/month=MM/day=DD/
            batch_<epoch>_<short-uuid>.parquet
    """

    def __init__(self) -> None:
        self._s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=S3_REGION,
        )
        self._bucket = S3_BUCKET_NAME
        self._total_files_written = 0
        self._total_records_written = 0

        logger.info(
            "S3 writer initialized -> s3://%s/%s | region: %s",
            self._bucket,
            S3_BRONZE_PREFIX,
            S3_REGION,
        )

    def write_batch(self, records: List[Dict[str, Any]]) -> bool:
        """
        Convert a list of trade dicts to a Parquet file and upload to S3.

        Parameters
        ----------
        records : list[dict]
            Trade messages deserialized from Kafka.

        Returns
        -------
        bool
            True if upload succeeded, False otherwise.
        """
        if not records:
            return True

        try:
            df = pd.DataFrame(records)

            # Parse source_timestamp to extract partition columns
            df["trade_time"] = pd.to_datetime(
                df["source_timestamp"], unit="ms", utc=True
            )
            df["year"] = df["trade_time"].dt.strftime("%Y")
            df["month"] = df["trade_time"].dt.strftime("%m")
            df["day"] = df["trade_time"].dt.strftime("%d")

            # Group by date partition and write separate files
            for (year, month, day), group_df in df.groupby(
                ["year", "month", "day"]
            ):
                partition_path = (
                    f"{S3_BRONZE_PREFIX}/"
                    f"year={year}/month={month}/day={day}/"
                )
                batch_id = str(uuid.uuid4())[:8]
                epoch = int(time.time())
                filename = f"batch_{epoch}_{batch_id}.parquet"
                s3_key = f"{partition_path}{filename}"

                # Drop helper columns before writing
                write_df = group_df.drop(
                    columns=["trade_time", "year", "month", "day"]
                )

                # Write Parquet to bytes buffer
                parquet_buffer = write_df.to_parquet(index=False)

                # Upload to S3
                self._s3_client.put_object(
                    Bucket=self._bucket,
                    Key=s3_key,
                    Body=parquet_buffer,
                    ContentType="application/octet-stream",
                )

                self._total_files_written += 1
                self._total_records_written += len(write_df)

                logger.info(
                    "S3 WRITE -> s3://%s/%s | records: %d | size: %.1f KB",
                    self._bucket,
                    s3_key,
                    len(write_df),
                    len(parquet_buffer) / 1024,
                )

            return True

        except ClientError as exc:
            logger.error("S3 upload failed: %s", exc)
            return False
        except Exception as exc:
            logger.error("Batch write error: %s", exc, exc_info=True)
            return False

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "total_files": self._total_files_written,
            "total_records": self._total_records_written,
        }


# ---------------------------------------------------------------------------
# Bronze Consumer
# ---------------------------------------------------------------------------
class BronzeStreamConsumer:
    """
    Kafka consumer that buffers trade messages and periodically
    flushes them to S3 as Parquet micro-batches.

    Flush triggers
    --------------
    1. Buffer reaches ``BATCH_SIZE`` records.
    2. ``FLUSH_INTERVAL_SEC`` seconds have elapsed since last flush.

    Kafka offsets are committed AFTER a successful S3 write,
    providing at-least-once delivery semantics.
    """

    def __init__(self) -> None:
        self._consumer = self._build_consumer()
        self._writer = S3ParquetWriter()
        self._buffer: List[Dict[str, Any]] = []
        self._last_flush_time: float = time.time()
        self._running: bool = False
        self._total_consumed: int = 0

    @staticmethod
    def _build_consumer() -> Consumer:
        """Create a confluent-kafka Consumer instance."""
        conf = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            # Disable auto-commit; we commit manually after S3 write
            "enable.auto.commit": False,
            # Performance tuning
            "fetch.min.bytes": 1024,
            "fetch.wait.max.ms": 500,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 30_000,
        }
        consumer = Consumer(conf)
        logger.info(
            "Kafka consumer initialized -> %s | group: %s | topic: %s",
            KAFKA_BOOTSTRAP_SERVERS,
            CONSUMER_GROUP_ID,
            KAFKA_TOPIC_TRADES,
        )
        return consumer

    def _should_flush(self) -> bool:
        """Check if the buffer should be flushed to S3."""
        if len(self._buffer) >= BATCH_SIZE:
            return True
        if time.time() - self._last_flush_time >= FLUSH_INTERVAL_SEC:
            return True
        return False

    def _flush_buffer(self) -> None:
        """Write buffered records to S3 and commit Kafka offsets."""
        if not self._buffer:
            self._last_flush_time = time.time()
            return

        batch_size = len(self._buffer)
        logger.info("Flushing buffer -> %d records to S3 ...", batch_size)

        success = self._writer.write_batch(self._buffer)

        if success:
            # Commit offsets only after successful write
            self._consumer.commit(asynchronous=False)
            self._buffer.clear()
            self._last_flush_time = time.time()
            logger.info(
                "Flush complete | Total consumed: %d | S3 stats: %s",
                self._total_consumed,
                self._writer.stats,
            )
        else:
            logger.error(
                "Flush FAILED - keeping %d records in buffer for retry",
                batch_size,
            )

    def start(self) -> None:
        """
        Subscribe to Kafka topic and begin consuming messages.

        Runs in an infinite loop until ``stop()`` is called.
        """
        self._running = True
        self._consumer.subscribe([KAFKA_TOPIC_TRADES])
        logger.info(
            "BronzeStreamConsumer started | "
            "batch_size=%d | flush_interval=%ds",
            BATCH_SIZE,
            FLUSH_INTERVAL_SEC,
        )

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # No message received - check time-based flush
                    if self._should_flush():
                        self._flush_buffer()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            "Reached end of partition %s [%d] at offset %d",
                            msg.topic(),
                            msg.partition(),
                            msg.offset(),
                        )
                    else:
                        logger.error("Consumer error: %s", msg.error())
                    continue

                # Deserialize and buffer the message
                try:
                    trade_data = json.loads(msg.value().decode("utf-8"))
                    self._buffer.append(trade_data)
                    self._total_consumed += 1
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    logger.warning(
                        "Skipped undecodable message at offset %d: %s",
                        msg.offset(),
                        exc,
                    )
                    continue

                # Check size-based flush
                if self._should_flush():
                    self._flush_buffer()

                # Periodic progress log
                if self._total_consumed % 1_000 == 0:
                    logger.info(
                        "Progress: %d messages consumed | buffer: %d",
                        self._total_consumed,
                        len(self._buffer),
                    )

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            # Final flush before shutdown
            self._flush_buffer()
            self._consumer.close()
            logger.info(
                "BronzeStreamConsumer stopped | "
                "Total consumed: %d | S3 stats: %s",
                self._total_consumed,
                self._writer.stats,
            )

    def stop(self) -> None:
        """Signal the consumer loop to stop."""
        logger.info("Stopping BronzeStreamConsumer ...")
        self._running = False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """Launch the Bronze stream consumer with graceful shutdown."""
    consumer = BronzeStreamConsumer()

    def _signal_handler(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, stopping ...", signum)
        consumer.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    consumer.start()


if __name__ == "__main__":
    main()
