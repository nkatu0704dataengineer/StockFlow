"""
Unit tests for StockQuoteSchema.
"""

import unittest

from include.core.schemas import StockQuoteSchema
from include.core.exceptions import SchemaValidationException


class TestStockQuoteSchema(unittest.TestCase):
    """Regression tests for quote schema construction and validation."""

    def test_schema_initializes_and_exports_dict(self):
        """A valid quote should initialize cleanly and export a canonical dictionary."""
        quote = StockQuoteSchema(
            symbol="NVDA",
            source_provider="Yahoo",
            timestamp=1718827200,
            open=124.0,
            high=126.5,
            low=123.1,
            close=125.43,
            volume=44958000,
            currency="USD",
            exchange="NASDAQ",
            ingested_at="2024-06-20T00:00:00+00:00",
            normalizer_version="1.0",
        )

        self.assertEqual(quote.symbol, "NVDA")
        self.assertEqual(quote.to_dict()["symbol"], "NVDA")
        self.assertEqual(quote.to_dict()["source_provider"], "Yahoo")

    def test_schema_rejects_blank_symbol(self):
        """Blank symbols must fail validation."""
        with self.assertRaises(SchemaValidationException):
            StockQuoteSchema(
                symbol="   ",
                source_provider="Yahoo",
                timestamp=1718827200,
                open=124.0,
                high=126.5,
                low=123.1,
                close=125.43,
                volume=44958000,
                currency="USD",
                exchange="NASDAQ",
                ingested_at="2024-06-20T00:00:00+00:00",
                normalizer_version="1.0",
            )

    def test_schema_rejects_non_numeric_ohlc(self):
        """OHLC fields must be numeric values."""
        with self.assertRaises(SchemaValidationException):
            StockQuoteSchema(
                symbol="NVDA",
                source_provider="Yahoo",
                timestamp=1718827200,
                open="bad",
                high=126.5,
                low=123.1,
                close=125.43,
                volume=44958000,
                currency="USD",
                exchange="NASDAQ",
                ingested_at="2024-06-20T00:00:00+00:00",
                normalizer_version="1.0",
            )

    def test_schema_rejects_non_integer_timestamp(self):
        """Timestamp must be an integer, not a float or string."""
        with self.assertRaises(SchemaValidationException):
            StockQuoteSchema(
                symbol="NVDA",
                source_provider="Yahoo",
                timestamp=1718827200.5,
                open=124.0,
                high=126.5,
                low=123.1,
                close=125.43,
                volume=44958000,
                currency="USD",
                exchange="NASDAQ",
                ingested_at="2024-06-20T00:00:00+00:00",
                normalizer_version="1.0",
            )

    def test_schema_allows_optional_provider_metadata_to_be_none(self):
        """Some providers may legitimately omit volume, currency, or exchange metadata."""
        quote = StockQuoteSchema(
            symbol="NVDA",
            source_provider="Finnhub",
            timestamp=1718827200,
            open=124.0,
            high=126.5,
            low=123.1,
            close=125.43,
            volume=None,
            currency=None,
            exchange=None,
            ingested_at="2024-06-20T00:00:00+00:00",
            normalizer_version="1.0",
        )

        self.assertIsNone(quote.volume)
        self.assertIsNone(quote.currency)
        self.assertIsNone(quote.exchange)


if __name__ == "__main__":
    unittest.main()
