"""
Unit tests for StockNormalizer.
"""

import unittest
from datetime import datetime

from include.api.normalizer import StockNormalizer
from include.core.exceptions import NormalizerException


class TestStockNormalizer(unittest.TestCase):
    """Regression tests for provider payload normalization."""

    def test_normalize_yahoo_payload(self):
        """Yahoo payloads must map into the unified quote schema."""
        normalizer = StockNormalizer()
        payload = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "NVDA",
                            "currency": "USD",
                            "exchangeName": "NASDAQ",
                        },
                        "timestamp": [1718827200],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [124.0],
                                    "high": [126.5],
                                    "low": [123.1],
                                    "close": [125.43],
                                    "volume": [44958000],
                                }
                            ]
                        },
                    }
                ]
            }
        }

        result = normalizer.normalize("Yahoo", payload, "NVDA")

        self.assertEqual(result["symbol"], "NVDA")
        self.assertEqual(result["source_provider"], "Yahoo")
        self.assertEqual(result["timestamp"], 1718827200)
        self.assertEqual(result["open"], 124.0)
        self.assertEqual(result["high"], 126.5)
        self.assertEqual(result["low"], 123.1)
        self.assertEqual(result["close"], 125.43)
        self.assertEqual(result["volume"], 44958000)
        self.assertEqual(result["currency"], "USD")
        self.assertEqual(result["exchange"], "NASDAQ")
        self.assertEqual(result["normalizer_version"], "1.0")
        self.assertIsInstance(result["ingested_at"], str)
        self.assertIn("T", result["ingested_at"])

    def test_normalize_finnhub_payload(self):
        """Finnhub payloads must map into the unified quote schema."""
        normalizer = StockNormalizer()
        payload = {
            "o": 124.0,
            "h": 126.5,
            "l": 123.1,
            "c": 125.43,
            "t": 1718827200,
        }

        result = normalizer.normalize("Finnhub", payload, "NVDA")

        self.assertEqual(result["symbol"], "NVDA")
        self.assertEqual(result["source_provider"], "Finnhub")
        self.assertEqual(result["timestamp"], 1718827200)
        self.assertEqual(result["open"], 124.0)
        self.assertEqual(result["high"], 126.5)
        self.assertEqual(result["low"], 123.1)
        self.assertEqual(result["close"], 125.43)
        self.assertIsNone(result["volume"])
        self.assertIsNone(result["currency"])
        self.assertIsNone(result["exchange"])
        self.assertEqual(result["normalizer_version"], "1.0")
        self.assertIsInstance(result["ingested_at"], str)

    def test_unknown_provider_raises_normalizer_exception(self):
        """Unsupported providers must be rejected by the normalizer."""
        normalizer = StockNormalizer()

        with self.assertRaises(NormalizerException):
            normalizer.normalize("Unknown", {"test": True}, "NVDA")

    def test_missing_yahoo_required_field_raises_normalizer_exception(self):
        """Yahoo normalization must fail clearly if a required quote field is missing."""
        normalizer = StockNormalizer()
        payload = {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "NVDA",
                            "currency": "USD",
                            "exchangeName": "NASDAQ",
                        },
                        "timestamp": [1718827200],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [124.0],
                                    "high": [126.5],
                                    "low": [123.1],
                                    "close": None,
                                    "volume": [44958000],
                                }
                            ]
                        },
                    }
                ]
            }
        }

        with self.assertRaises(NormalizerException):
            normalizer.normalize("Yahoo", payload, "NVDA")


if __name__ == "__main__":
    unittest.main()
