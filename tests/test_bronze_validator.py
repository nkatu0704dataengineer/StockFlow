"""Regression tests for BronzeValidator.

These tests validate only the Bronze validator's contract: inspect a Spark
DataFrame, reject missing columns, reject invalid datatypes, reject
identifier duplicates, and return the exact input DataFrame when all checks
pass.
"""

from __future__ import annotations

import unittest

from include.core.exceptions import BronzeValidatorException
from include.framework.bronze.validator import BronzeValidator


class FakeGroupedDataFrame:
    """Minimal grouped DataFrame for duplicate detection tests."""

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows

    def count(self) -> int:
        return len(self.rows)

    def collect(self) -> list[dict[str, object]]:
        return self.rows


class FakeDataFrame:
    """Minimal Spark-like DataFrame with schema metadata and duplicates API."""

    REQUIRED_COLUMNS = [
        "symbol",
        "source_provider",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "currency",
        "exchange",
        "ingested_at",
        "normalizer_version",
    ]

    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self._rows = rows or []
        self.columns = self.REQUIRED_COLUMNS
        self.dtypes = [
            ("symbol", "string"),
            ("source_provider", "string"),
            ("timestamp", "bigint"),
            ("open", "double"),
            ("high", "double"),
            ("low", "double"),
            ("close", "double"),
            ("volume", "double"),
            ("currency", "string"),
            ("exchange", "string"),
            ("ingested_at", "string"),
            ("normalizer_version", "string"),
        ]

    def groupBy(self, *columns: str) -> FakeGroupedDataFrame:
        if not columns:
            return FakeGroupedDataFrame([])

        grouped: dict[tuple[object, ...], int] = {}
        for row in self._rows:
            key = tuple(row[column] for column in columns)
            grouped[key] = grouped.get(key, 0) + 1

        duplicates = [
            {"symbol": key[0], "timestamp": key[1], "count": count}
            for key, count in grouped.items()
            if count > 1
        ]
        return FakeGroupedDataFrame(duplicates)


class TestBronzeValidator(unittest.TestCase):
    """Regression tests for the BronzeValidator contract."""

    def test_validate_returns_original_dataframe_when_all_checks_pass(self) -> None:
        validator = BronzeValidator()
        dataframe = FakeDataFrame(rows=[{
            "symbol": "AAPL",
            "source_provider": "Finnhub",
            "timestamp": 1718827200,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1234,
            "currency": "USD",
            "exchange": "NASDAQ",
            "ingested_at": "2024-06-20T00:00:00Z",
            "normalizer_version": "1.0",
        }])

        result = validator.validate(dataframe)
        self.assertIs(result, dataframe)

    def test_validate_raises_on_missing_required_columns(self) -> None:
        validator = BronzeValidator()
        dataframe = FakeDataFrame(rows=[])
        dataframe.columns = ["symbol", "timestamp", "open", "high", "low", "close"]

        with self.assertRaises(BronzeValidatorException):
            validator.validate(dataframe)

    def test_validate_raises_on_invalid_datatypes(self) -> None:
        validator = BronzeValidator()
        dataframe = FakeDataFrame(rows=[])
        dataframe.dtypes = [
            ("symbol", "integer"),
            ("source_provider", "string"),
            ("timestamp", "string"),
            ("open", "string"),
            ("high", "double"),
            ("low", "double"),
            ("close", "double"),
            ("volume", "string"),
            ("currency", "integer"),
            ("exchange", "string"),
            ("ingested_at", "string"),
            ("normalizer_version", "string"),
        ]

        with self.assertRaises(BronzeValidatorException):
            validator.validate(dataframe)

    def test_validate_raises_on_duplicate_symbol_timestamp_pairs(self) -> None:
        validator = BronzeValidator()
        dataframe = FakeDataFrame(rows=[
            {
                "symbol": "AAPL",
                "source_provider": "Finnhub",
                "timestamp": 1718827200,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1234,
                "currency": "USD",
                "exchange": "NASDAQ",
                "ingested_at": "2024-06-20T00:00:00Z",
                "normalizer_version": "1.0",
            },
            {
                "symbol": "AAPL",
                "source_provider": "Yahoo",
                "timestamp": 1718827200,
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 1234,
                "currency": "USD",
                "exchange": "NASDAQ",
                "ingested_at": "2024-06-20T00:00:01Z",
                "normalizer_version": "1.0",
            },
        ])

        with self.assertRaises(BronzeValidatorException):
            validator.validate(dataframe)


if __name__ == "__main__":
    unittest.main()
