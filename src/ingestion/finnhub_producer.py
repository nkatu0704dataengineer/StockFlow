"""
Finnhub WebSocket Kafka Producer.

Connects to Finnhub's real-time WebSocket feed, normalizes each
incoming trade into the StockFlow TradeMessage schema, and publishes
it to the ``finnhub_realtime_trades`` Kafka topic.

Usage
-----
    python -m src.ingestion.finnhub_producer

The producer runs indefinitely until interrupted (Ctrl+C).
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from typing import Any

import websocket
from confluent_kafka import Producer, KafkaError

from src.config.settings import (
    FINNHUB_API_KEY,
    FINNHUB_WS_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_TRADES,
    WATCHLIST,
)
from src.schemas.trade_schema import TradeMessage


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stockflow.producer.finnhub")


# ---------------------------------------------------------------------------
# Kafka helpers
# ---------------------------------------------------------------------------
def _build_kafka_producer() -> Producer:
    """
    Create and return a confluent-kafka Producer instance.

    The configuration intentionally keeps things simple for local dev.
    Production deployments should layer on TLS, SASL, and idempotence.
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "stockflow-finnhub-producer",
        # Reliability: wait for leader acknowledgement
        "acks": "all",
        # Batching: small latency budget suitable for real-time
        "linger.ms": 5,
        "batch.size": 16384,
        # Compression: reduce network I/O
        "compression.type": "snappy",
    }
    producer = Producer(conf)
    logger.info(
        "Kafka producer initialized -> %s | topic: %s",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC_TRADES,
    )
    return producer


def _delivery_callback(err: KafkaError | None, msg: Any) -> None:
    """Log Kafka delivery result (success or failure)."""
    if err is not None:
        logger.error("DELIVERY FAILED [%s]: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered -> topic=%s partition=%s offset=%s key=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.key(),
        )


# ---------------------------------------------------------------------------
# WebSocket callbacks
# ---------------------------------------------------------------------------
class FinnhubProducer:
    """
    Bridges Finnhub WebSocket to Kafka.

    Lifecycle
    ---------
    1. ``start()``  -> opens the WS connection & subscribes to symbols.
    2. ``on_message()`` -> fires on every incoming WS frame, normalizes
       data, and publishes each trade to Kafka.
    3. ``stop()``   -> gracefully closes WS + flushes Kafka buffer.
    """

    def __init__(self) -> None:
        if not FINNHUB_API_KEY:
            raise RuntimeError(
                "FINNHUB_API_KEY is not set. "
                "Please add it to your .env file."
            )

        self._producer: Producer = _build_kafka_producer()
        self._ws: websocket.WebSocketApp | None = None
        self._running: bool = False
        self._msg_count: int = 0
        self._error_count: int = 0

    # ---- WebSocket event handlers ----------------------------------------

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        """Subscribe to every symbol in the watchlist upon connection."""
        logger.info("WebSocket connected to Finnhub")
        for symbol in WATCHLIST:
            payload = json.dumps({"type": "subscribe", "symbol": symbol})
            ws.send(payload)
            logger.info("Subscribed -> %s", symbol)

    def _on_message(self, ws: websocket.WebSocketApp, raw_message: str) -> None:
        """
        Parse incoming Finnhub frame, normalize each trade, publish to Kafka.

        Finnhub sends two types of frames:
        - ``{"type": "ping"}``  -> heartbeat, ignored.
        - ``{"type": "trade", "data": [...]}`` -> trade events to process.
        """
        try:
            payload = json.loads(raw_message)

            # Ignore heartbeat pings
            if payload.get("type") != "trade":
                return

            trades: list[dict] = payload.get("data", [])
            for raw_trade in trades:
                message = TradeMessage.from_finnhub(raw_trade)
                self._producer.produce(
                    topic=KAFKA_TOPIC_TRADES,
                    key=TradeMessage.key_bytes(message.symbol),
                    value=message.to_bytes(),
                    callback=_delivery_callback,
                )
                self._msg_count += 1

            # Trigger delivery callbacks without blocking
            self._producer.poll(0)

            # Periodic progress log every 500 messages
            if self._msg_count % 500 == 0 and self._msg_count > 0:
                logger.info(
                    "Progress: %d messages published to Kafka", self._msg_count
                )

        except (KeyError, ValueError, TypeError) as exc:
            self._error_count += 1
            logger.warning("Skipped malformed trade: %s | raw=%s", exc, raw_message[:200])

    def _on_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        """Log WebSocket errors."""
        self._error_count += 1
        logger.error("WebSocket error: %s", error)

    def _on_close(self, ws: websocket.WebSocketApp, close_code: int, close_msg: str) -> None:
        """Handle WebSocket disconnect."""
        logger.warning(
            "WebSocket closed (code=%s, msg=%s). Total published: %d, errors: %d",
            close_code,
            close_msg,
            self._msg_count,
            self._error_count,
        )

    # ---- Public API ------------------------------------------------------

    def start(self) -> None:
        """
        Open the WebSocket connection and run the event loop.

        This method blocks until ``stop()`` is called or the process
        receives SIGINT / SIGTERM.
        """
        self._running = True
        logger.info("Starting FinnhubProducer | watchlist=%s", WATCHLIST)

        self._ws = websocket.WebSocketApp(
            FINNHUB_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        # run_forever with auto-reconnect (reconnect after 5s on disconnect)
        self._ws.run_forever(
            reconnect=5,
            ping_interval=30,
            ping_timeout=10,
        )

    def stop(self) -> None:
        """Gracefully shut down WebSocket and flush Kafka buffer."""
        logger.info("Shutting down FinnhubProducer ...")
        self._running = False

        if self._ws:
            self._ws.close()

        # Flush remaining messages (wait up to 10 seconds)
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            logger.warning("%d messages were NOT delivered before shutdown", remaining)

        logger.info(
            "Shutdown complete. Total published: %d | Errors: %d",
            self._msg_count,
            self._error_count,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """Launch the Finnhub -> Kafka producer with graceful shutdown."""
    producer = FinnhubProducer()

    def _signal_handler(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, stopping ...", signum)
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    producer.start()


if __name__ == "__main__":
    main()
