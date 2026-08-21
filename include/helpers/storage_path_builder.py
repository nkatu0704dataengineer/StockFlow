"""
Storage path builder.

Centralized helper for constructing Bronze storage paths.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


class StoragePathBuilder:
    """
    Build storage paths for Bronze layer.
    """

    BUCKET = "stockflow"
    LAYER = "bronze"

    @classmethod
    def build_bronze_path(cls, quote: dict[str, Any]) -> str:
        """
        Build Bronze storage path from a normalized quote.

        Parameters
        ----------
        quote : dict
            Normalized quote returned by StockNormalizer.

        Returns
        -------
        str
            Fully-qualified s3a path.
        """

        provider = quote["source_provider"].lower()
        symbol = quote["symbol"].upper()

        timestamp = quote["timestamp"]

        # Finnhub trả Unix timestamp
        if isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp)

        # ISO string
        elif isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(
                timestamp.replace("Z", "+00:00")
            )

        if not isinstance(timestamp, datetime):
            raise TypeError(
                "Normalized quote contains an invalid timestamp."
            )

        return (
            f"s3a://{cls.BUCKET}/"
            f"{cls.LAYER}/"
            f"provider={provider}/"
            f"year={timestamp:%Y}/"
            f"month={timestamp:%m}/"
            f"day={timestamp:%d}/"
            f"symbol={symbol}"
        )