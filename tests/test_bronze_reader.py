"""Regression tests for BronzeReader.

These tests exercise only the reader's responsibility: converting Python
objects into a Spark DataFrame without applying validation or business
transformation.
"""

from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock


pyspark_stub = types.ModuleType("pyspark")
sql_stub = types.ModuleType("pyspark.sql")


class DataFrame:
    """Minimal DataFrame placeholder used for reader unit tests."""

    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows or []


class SparkSession:
    """Minimal SparkSession placeholder used for reader unit tests."""

    def __init__(self) -> None:
        self.createDataFrame = MagicMock(side_effect=lambda rows: DataFrame(rows))


sql_stub.SparkSession = SparkSession
sql_stub.DataFrame = DataFrame
sys.modules.setdefault("pyspark", pyspark_stub)
sys.modules.setdefault("pyspark.sql", sql_stub)

from include.core.exceptions import BronzeReaderException
from include.core.schemas import StockQuoteSchema
from include.framework.bronze.reader import BronzeReader


class TestBronzeReader(unittest.TestCase):
    """Verification for BronzeReader's input dispatch and Spark conversion."""

    def test_read_schema_object_returns_dataframe(self) -> None:
        spark = SparkSession()
        reader = BronzeReader(spark=spark)
        schema = StockQuoteSchema(
            symbol="AAPL",
            source_provider="Finnhub",
            timestamp=1718827200,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000,
            currency="USD",
            exchange="NASDAQ",
            ingested_at="2024-06-20T00:00:00Z",
            normalizer_version="1.0",
        )

        result = reader.read(schema)
        self.assertIsInstance(result, DataFrame)
        self.assertEqual(result.rows, [schema.to_dict()])

    def test_read_dict_returns_single_row_dataframe(self) -> None:
        spark = SparkSession()
        reader = BronzeReader(spark=spark)
        payload = {
            "symbol": "MSFT",
            "source_provider": "Yahoo",
            "timestamp": 1718827200,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000,
            "currency": "USD",
            "exchange": "NASDAQ",
            "ingested_at": "2024-06-20T00:00:00Z",
            "normalizer_version": "1.0",
        }

        result = reader.read(payload)
        self.assertIsInstance(result, DataFrame)
        self.assertEqual(result.rows, [payload])

    def test_read_empty_list_raises(self) -> None:
        spark = SparkSession()
        reader = BronzeReader(spark=spark)

        with self.assertRaises(BronzeReaderException):
            reader.read([])

    def test_read_unsupported_input_raises(self) -> None:
        spark = SparkSession()
        reader = BronzeReader(spark=spark)

        with self.assertRaises(BronzeReaderException):
            reader.read(123)


if __name__ == "__main__":
    unittest.main()
