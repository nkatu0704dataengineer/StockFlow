"""Regression tests for BronzeMetadata.

These tests verify that the metadata layer enriches a Spark DataFrame with
Bronze-specific fields without mutating the source quote identity columns.
"""

from __future__ import annotations

import unittest
from uuid import UUID

from include.core.exceptions import BronzeMetadataException
from include.framework.bronze.metadata import BronzeMetadata


class FakeDataFrame:
    """Minimal Spark-like DataFrame stub used for metadata tests."""

    def __init__(self) -> None:
        self.columns = [
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
        self._added_columns: dict[str, object] = {}

    def withColumn(self, column_name: str, value: object) -> "FakeDataFrame":
        self._added_columns[column_name] = value
        self.columns.append(column_name)
        return self


class TestBronzeMetadata(unittest.TestCase):
    """Verification for BronzeMetadata's enrichment contract."""

    def test_enrich_adds_required_bronze_metadata_columns(self) -> None:
        metadata = BronzeMetadata()
        dataframe = FakeDataFrame()

        result = metadata.enrich(dataframe)

        self.assertIs(result, dataframe)
        self.assertIn("bronze_ingested_at", dataframe.columns)
        self.assertIn("bronze_batch_id", dataframe.columns)
        self.assertIn("bronze_job_name", dataframe.columns)
        self.assertIn("bronze_layer", dataframe.columns)
        self.assertIn("bronze_version", dataframe.columns)
        self.assertIn("run_id", dataframe.columns)

        self.assertTrue(UUID(str(dataframe._added_columns["bronze_batch_id"])))
        self.assertTrue(UUID(str(dataframe._added_columns["run_id"])))
        self.assertEqual(dataframe._added_columns["bronze_job_name"], "bronze_pipeline")
        self.assertEqual(dataframe._added_columns["bronze_layer"], "bronze")
        self.assertEqual(dataframe._added_columns["bronze_version"], "1.0")

    def test_enrich_rejects_non_dataframe_input(self) -> None:
        metadata = BronzeMetadata()

        with self.assertRaises(BronzeMetadataException):
            metadata.enrich(object())


if __name__ == "__main__":
    unittest.main()
