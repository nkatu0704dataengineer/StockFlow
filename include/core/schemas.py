"""
StockFlow API Schemas.

This module defines the canonical in-memory schema objects used by the
StockFlow platform.

The schema layer is the single source of truth for validated downstream
consumption. It is intentionally kept independent from provider fetch,
retry orchestration, normalization, Spark, storage, and file writing.
"""

from __future__ import annotations

from typing import Any

from include.core.exceptions import SchemaValidationException


class StockQuoteSchema:
    """
    Canonical StockFlow market quote schema.

    This object stores one validated normalized quote record and exposes it
    as a dictionary for downstream processing layers.

    Parameters
    ----------
    symbol : str
        Uppercase stock ticker symbol.
    source_provider : str
        Provider name that produced the quote.
    timestamp : int
        Unix timestamp of the quote.
    open : float
        Opening price.
    high : float
        Highest price.
    low : float
        Lowest price.
    close : float
        Closing price.
    volume : int | None
        Volume value when supplied by the provider.
    currency : str | None
        Quote currency when available.
    exchange : str | None
        Exchange name when available.
    ingested_at : str
        UTC ISO8601 timestamp captured during normalization.
    normalizer_version : str
        Normalizer schema version marker.
    """

    REQUIRED_FIELDS = (
        "symbol",
        "source_provider",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "ingested_at",
        "normalizer_version",
    )

    OPTIONAL_FIELDS = (
        "volume",
        "currency",
        "exchange",
    )

    def __init__(
        self,
        symbol: str,
        source_provider: str,
        timestamp: int,
        open: float,
        high: float,
        low: float,
        close: float,
        volume: int | None = None,
        currency: str | None = None,
        exchange: str | None = None,
        ingested_at: str = "",
        normalizer_version: str = "1.0",
    ) -> None:
        """
        Initialize the quote schema and validate it immediately.

        Parameters
        ----------
        symbol : str
            Uppercase ticker symbol.
        source_provider : str
            Provider name.
        timestamp : int
            Unix timestamp.
        open : float
            Opening price.
        high : float
            Highest price.
        low : float
            Lowest price.
        close : float
            Closing price.
        volume : int | None, optional
            Volume when available.
        currency : str | None, optional
            Quote currency when available.
        exchange : str | None, optional
            Exchange name when available.
        ingested_at : str, optional
            UTC ISO8601 timestamp.
        normalizer_version : str, optional
            Normalizer version marker.
        """
        self.symbol = symbol
        self.source_provider = source_provider
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.currency = currency
        self.exchange = exchange
        self.ingested_at = ingested_at
        self.normalizer_version = normalizer_version

        self.validate()

    def validate(self) -> None:
        """
        Validate the schema instance before it is used downstream.

        Raises
        ------
        SchemaValidationException
            If any required field is missing, empty, or invalid.
        """
        self._validate_required_string_fields()
        self._validate_numeric_fields()
        self._validate_optional_string_fields()
        self._validate_required_scalar_fields()

    def to_dict(self) -> dict[str, Any]:
        """
        Export the schema instance as a dictionary.

        Returns
        -------
        dict[str, Any]
            Canonical quote payload for downstream layers.
        """
        return {
            "symbol": self.symbol,
            "source_provider": self.source_provider,
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "currency": self.currency,
            "exchange": self.exchange,
            "ingested_at": self.ingested_at,
            "normalizer_version": self.normalizer_version,
        }

    def __repr__(self) -> str:
        """
        Return a readable representation of the schema instance.

        Returns
        -------
        str
            Human-readable debugging summary.
        """
        return (
            "StockQuoteSchema("
            f"symbol={self.symbol!r}, "
            f"source_provider={self.source_provider!r}, "
            f"timestamp={self.timestamp!r}, "
            f"open={self.open!r}, "
            f"high={self.high!r}, "
            f"low={self.low!r}, "
            f"close={self.close!r}, "
            f"volume={self.volume!r}, "
            f"currency={self.currency!r}, "
            f"exchange={self.exchange!r}, "
            f"ingested_at={self.ingested_at!r}, "
            f"normalizer_version={self.normalizer_version!r}"
            ")"
        )

    def _validate_required_string_fields(self) -> None:
        """
        Validate required string attributes for the quote schema.

        Raises
        ------
        SchemaValidationException
            If any required string field is missing or blank.
        """
        for field_name in ("symbol", "source_provider", "ingested_at", "normalizer_version"):
            value = getattr(self, field_name)
            self._ensure_required_string(field_name, value)

    def _validate_optional_string_fields(self) -> None:
        """
        Validate optional string attributes when they are provided.

        Raises
        ------
        SchemaValidationException
            If an optional string field is present but blank.
        """
        for field_name in ("currency", "exchange"):
            value = getattr(self, field_name)
            if value is not None:
                self._ensure_required_string(field_name, value)

    def _validate_required_scalar_fields(self) -> None:
        """
        Validate required scalar fields for the quote schema.

        Raises
        ------
        SchemaValidationException
            If timestamp is not an integer or OHLC values are not numeric.
        """
        if not isinstance(self.timestamp, int) or isinstance(self.timestamp, bool):
            raise SchemaValidationException("Schema validation failed: 'timestamp' must be an integer.")

        numeric_fields = ("open", "high", "low", "close")
        for field_name in numeric_fields:
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SchemaValidationException(
                    f"Schema validation failed: '{field_name}' must be numeric."
                )

    def _validate_numeric_fields(self) -> None:
        """
        Validate numeric and optional numeric fields.

        Raises
        ------
        SchemaValidationException
            If the optional volume field is provided but is not numeric.
        """
        if self.volume is not None:
            if isinstance(self.volume, bool) or not isinstance(self.volume, (int, float)):
                raise SchemaValidationException(
                    "Schema validation failed: 'volume' must be numeric when provided."
                )

    def _ensure_required_string(self, field_name: str, value: Any) -> None:
        """
        Ensure a field is present and not blank.

        Parameters
        ----------
        field_name : str
            Name of the field being validated.
        value : Any
            Field value to inspect.

        Raises
        ------
        SchemaValidationException
            If the value is None, blank, or otherwise invalid.
        """
        if value is None:
            raise SchemaValidationException(
                f"Schema validation failed: '{field_name}' cannot be None."
            )

        if not isinstance(value, str):
            raise SchemaValidationException(
                f"Schema validation failed: '{field_name}' must be a string."
            )

        if not value.strip():
            raise SchemaValidationException(
                f"Schema validation failed: '{field_name}' cannot be empty."
            )

        if field_name == "symbol" and value != value.strip().upper():
            raise SchemaValidationException(
                "Schema validation failed: 'symbol' must be uppercase."
            )
