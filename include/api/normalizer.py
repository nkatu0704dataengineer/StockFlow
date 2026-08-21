"""
StockFlow Normalizer.

The normalizer ONLY maps provider payloads into the unified StockFlow schema.
It NEVER:
- calls APIs
- performs retries
- calculates indicators
- transforms business logic
- accesses Spark
- accesses storage
- writes files

Financial calculations belong to downstream transformation layers.
"""

from __future__ import annotations

from curses import raw
import logging
from datetime import datetime, timezone
from typing import Any

from include.core.exceptions import NormalizerException


class StockNormalizer:
    """
    Normalize provider-specific raw JSON into the unified StockFlow quote schema.

    Parameters
    ----------
    None

    Notes
    -----
    The normalizer dispatches by provider name and delegates the provider-
    specific mapping to private helper methods.
    """

    NORMALIZER_VERSION = "1.0"
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

    def __init__(self) -> None:
        """Initialize the normalizer with a module-scoped logger."""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def normalize(self, provider_name: str, raw_json: dict, symbol: str) -> dict:
        """
        Normalize provider raw JSON into the StockFlow unified quote schema.

        Parameters
        ----------
        provider_name : str
            Provider display name used to select the mapping strategy.
        raw_json : dict
            Raw provider payload returned from the provider layer.
        symbol : str
            Stock ticker symbol to associate with the normalized record.

        Returns
        -------
        dict
            Unified StockFlow quote schema.

        Raises
        ------
        NormalizerException
            If the provider is unknown, the payload is malformed, or required
            fields are missing.
        """
        normalized_symbol = symbol.strip().upper()
        provider_key = provider_name.strip().lower()

        self.logger.info(
            "normalization_started",
            extra={
                "provider": provider_name,
                "symbol": normalized_symbol,
            },
        )

        dispatch = {
            "yahoo": self._normalize_yahoo,
            "yahoo finance": self._normalize_yahoo,
            "finnhub": self._normalize_finnhub,
        }
        normalizer = dispatch.get(provider_key)
        if normalizer is None:
            raise NormalizerException(
                f"Unsupported provider '{provider_name}' for normalization."
            )

        normalized = normalizer(
            raw_json=raw_json,
            symbol=normalized_symbol,
            provider_name=provider_name,
        )

        self._validate_normalized_payload(normalized)
        self.logger.info(
            "normalization_succeeded",
            extra={
                "provider": provider_name,
                "symbol": normalized_symbol,
            },
        )
        return normalized

    def _normalize_yahoo(self, raw_json: dict, symbol: str, provider_name: str) -> dict:
        """
        Normalize a Yahoo Finance payload into the unified quote schema.

        Parameters
        ----------
        raw_json : dict
            Raw Yahoo Finance payload.
        symbol : str
            Stock ticker symbol.

        Returns
        -------
        dict
            Unified StockFlow quote schema.

        Raises
        ------
        NormalizerException
            If the Yahoo payload is missing required fields.
        """
        try:
            chart_result = raw_json["chart"]["result"][0]
            meta = chart_result["meta"]
            timestamp_values = chart_result["timestamp"]
            quote_entry = chart_result["indicators"]["quote"][0]
            open_values = quote_entry["open"]
            high_values = quote_entry["high"]
            low_values = quote_entry["low"]
            close_values = quote_entry["close"]
            volume_values = quote_entry["volume"]
        except (KeyError, IndexError, TypeError) as exc:
            raise NormalizerException(
                "Yahoo payload is missing required chart metadata or quote fields."
            ) from exc

        meta_symbol = self._require_field(meta, "symbol")
        currency = self._require_field(meta, "currency")
        exchange_name = self._require_field(meta, "exchangeName")
        timestamp = self._extract_scalar(timestamp_values, "timestamp")
        open_value = self._extract_scalar(open_values, "open")
        high_value = self._extract_scalar(high_values, "high")
        low_value = self._extract_scalar(low_values, "low")
        close_value = self._extract_scalar(close_values, "close")
        volume_value = self._extract_scalar(volume_values, "volume")

        if str(meta_symbol).strip().upper() != symbol:
            raise NormalizerException(
                "Yahoo metadata symbol does not match the requested symbol."
            )

        normalized = {
            "symbol": symbol,
            "source_provider": provider_name,
            "timestamp": timestamp,
            "open": open_value,
            "high": high_value,
            "low": low_value,
            "close": close_value,
            "volume": volume_value,
            "currency": currency,
            "exchange": exchange_name,
            "ingested_at": self._now_iso(),
            "normalizer_version": self.NORMALIZER_VERSION,
        }

        return normalized

    def _normalize_finnhub(self, raw_json: dict, symbol: str, provider_name: str) -> dict:
        """
        Normalize a Finnhub quote payload into the unified quote schema.

        Parameters
        ----------
        raw_json : dict
            Raw Finnhub quote payload.
        symbol : str
            Stock ticker symbol.

        Returns
        -------
        dict
            Unified StockFlow quote schema.

        Raises
        ------
        NormalizerException
            If the Finnhub payload is missing required fields.
        """
        try:
            timestamp = self._require_field(raw_json, "t")
            open_value = self._require_field(raw_json, "o")
            high_value = self._require_field(raw_json, "h")
            low_value = self._require_field(raw_json, "l")
            close_value = self._require_field(raw_json, "c")
        except NormalizerException as exc:
            raise NormalizerException(
                "Finnhub payload is missing required quote fields."
            ) from exc

        normalized = {
            "symbol": symbol,
            "source_provider": provider_name,
            "timestamp": timestamp,
            "open": float(open_value or 0.0),
            "high": float(high_value or 0.0),
            "low": float(low_value or 0.0),
            "close": float(close_value or 0.0),

            # Spark infer được kiểu
            "volume": int(raw_json.get("v") or 0),

            # luôn StringType
            "currency": "",

            # luôn StringType
            "exchange": "",

            "ingested_at": self._now_iso(),
            "normalizer_version": self.NORMALIZER_VERSION,
        }
        return normalized

    def _now_iso(self) -> str:
        """
        Return the current UTC timestamp as an ISO8601 string.

        Returns
        -------
        str
            Current UTC timestamp in ISO8601 format.
        """
        return datetime.now(timezone.utc).isoformat()

    def _extract_scalar(self, values: list[Any], field_name: str) -> Any:
        """
        Extract the first scalar value from a provider array payload.

        Parameters
        ----------
        values : list[Any]
            List-like payload returned by a provider.
        field_name : str
            Field name used for error reporting.

        Returns
        -------
        Any
            First scalar value from the list.

        Raises
        ------
        NormalizerException
            If the list is empty or the extracted value is missing.
        """
        if not values or len(values) == 0:
            raise NormalizerException(f"Missing required Yahoo field '{field_name}'.")

        value = values[0]
        if value is None:
            raise NormalizerException(f"Missing required Yahoo field '{field_name}'.")

        return value

    def _require_field(self, payload: dict, field_name: str) -> Any:
        """
        Return a payload field when present, otherwise raise a normalization error.

        Parameters
        ----------
        payload : dict
            Provider payload dictionary.
        field_name : str
            Required field name.

        Returns
        -------
        Any
            Field value when present.

        Raises
        ------
        NormalizerException
            If the field is missing.
        """
        if field_name not in payload:
            raise NormalizerException(f"Missing required field '{field_name}'.")

        value = payload[field_name]
        if value is None:
            raise NormalizerException(f"Missing required field '{field_name}'.")

        return value

    def _validate_normalized_payload(self, normalized: dict) -> None:
        """
        Validate the normalized schema before returning it to callers.

        Parameters
        ----------
        normalized : dict
            Unified quote schema dictionary.

        Raises
        ------
        NormalizerException
            If any required top-level field is missing or empty.
        """
        for field_name in self.REQUIRED_FIELDS:
            if field_name not in normalized:
                raise NormalizerException(f"Missing required normalized field '{field_name}'.")

            value = normalized[field_name]
            if value is None:
                raise NormalizerException(f"Missing required normalized field '{field_name}'.")

            if isinstance(value, str) and not value.strip():
                raise NormalizerException(
                    f"Missing required normalized field '{field_name}'."
                )
