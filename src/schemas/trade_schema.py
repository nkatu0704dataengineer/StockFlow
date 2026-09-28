"""
Kafka message schema definitions for StockFlow.

Defines the standardized trade message structure used across
the entire pipeline: Producer -> Kafka -> Spark Consumer -> S3.
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field, asdict
from typing import Optional
import json


@dataclass(frozen=True)
class TradeMessage:
    """
    Normalized trade message published to Kafka.

    This schema transforms Finnhub's compact WebSocket payload
    (single-letter keys like 'p', 's', 't', 'v') into a
    human-readable, self-documenting structure.

    Attributes
    ----------
    event_id : str
        UUID v4 for deduplication in downstream Spark jobs.
    symbol : str
        Stock ticker symbol (e.g., 'AAPL', 'TSLA').
    price : float
        Trade execution price.
    volume : float
        Number of shares traded.
    trade_conditions : list[str]
        Exchange-specific trade condition codes.
    source_timestamp : int
        Epoch milliseconds when the trade occurred on the exchange
        (provided by Finnhub).
    ingestion_timestamp : int
        Epoch milliseconds when our Producer received and published
        the message. Used to measure system latency.
    """

    symbol: str
    price: float
    volume: float
    source_timestamp: int
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    trade_conditions: list[str] = field(default_factory=list)
    ingestion_timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_json(self) -> str:
        """Serialize to JSON string for Kafka producer."""
        return json.dumps(asdict(self), ensure_ascii=False)

    def to_bytes(self) -> bytes:
        """Serialize to UTF-8 bytes for Kafka producer value."""
        return self.to_json().encode("utf-8")

    @staticmethod
    def key_bytes(symbol: str) -> bytes:
        """
        Generate the Kafka message key from the symbol.

        Using symbol as key ensures all trades for the same ticker
        land on the same Kafka partition, preserving time ordering.
        """
        return symbol.encode("utf-8")

    @classmethod
    def from_finnhub(cls, raw: dict) -> TradeMessage:
        """
        Factory: build a TradeMessage from a single Finnhub trade object.

        Finnhub WebSocket sends:
            {"p": 150.25, "s": "AAPL", "t": 1629837237123, "v": 12.5, "c": ["1"]}

        Parameters
        ----------
        raw : dict
            A single element from the Finnhub 'data' array.

        Returns
        -------
        TradeMessage
            Normalized, enriched trade message ready for Kafka.
        """
        return cls(
            symbol=raw["s"],
            price=float(raw["p"]),
            volume=float(raw["v"]),
            source_timestamp=int(raw["t"]),
            trade_conditions=raw.get("c", []),
        )
