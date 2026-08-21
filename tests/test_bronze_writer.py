"""Regression tests for BronzeWriter.

These tests verify that the writer accepts only a validated Spark DataFrame
and persists it using the requested output format and write mode, then
returns the target path.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from include.core.exceptions import BronzeWriterException
from include.framework.bronze.writer import BronzeWriter


class FakeDataFrameWriter:
    """Minimal Spark DataFrameWriter stub used by the writer tests."""

    def __init__(self) -> None:
        self.selected_mode = None
        self.selected_partition_by = None
        self.selected_format_name = None
        self.saved_output_path = None

    def mode(self, mode_value: str):
        self.selected_mode = mode_value
        return self

    def partitionBy(self, partition_by: list[str] | None):
        self.selected_partition_by = partition_by
        return self

    def format(self, format_name: str):
        self.selected_format_name = format_name
        return self

    def save(self, output_path: str):
        self.saved_output_path = output_path
        return None


class FakeDataFrame:
    """Minimal Spark-like DataFrame exposing a DataFrameWriter surface."""

    def __init__(self) -> None:
        self._writer = FakeDataFrameWriter()

    def write(self):
        return self._writer


class TestBronzeWriter(unittest.TestCase):
    """Verification for BronzeWriter's persistence contract."""

    def test_write_parquet_returns_output_path(self) -> None:
        writer = BronzeWriter()
        dataframe = FakeDataFrame()

        result = writer.write(dataframe, "/tmp/test-output", format="parquet", mode="append")

        self.assertEqual(result, "/tmp/test-output")
        self.assertEqual(dataframe._writer.selected_mode, "append")
        self.assertEqual(dataframe._writer.selected_format_name, "parquet")
        self.assertEqual(dataframe._writer.saved_output_path, "/tmp/test-output")

    def test_write_delta_returns_output_path(self) -> None:
        writer = BronzeWriter()
        dataframe = FakeDataFrame()

        result = writer.write(dataframe, "/tmp/test-output", format="delta", mode="overwrite")

        self.assertEqual(result, "/tmp/test-output")
        self.assertEqual(dataframe._writer.selected_mode, "overwrite")
        self.assertEqual(dataframe._writer.selected_format_name, "delta")
        self.assertEqual(dataframe._writer.saved_output_path, "/tmp/test-output")

    def test_write_invalid_format_raises(self) -> None:
        writer = BronzeWriter()
        dataframe = FakeDataFrame()

        with self.assertRaises(BronzeWriterException):
            writer.write(dataframe, "/tmp/test-output", format="csv")

    def test_write_invalid_mode_raises(self) -> None:
        writer = BronzeWriter()
        dataframe = FakeDataFrame()

        with self.assertRaises(BronzeWriterException):
            writer.write(dataframe, "/tmp/test-output", format="parquet", mode="merge")


if __name__ == "__main__":
    unittest.main()
