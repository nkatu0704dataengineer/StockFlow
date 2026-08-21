"""Regression tests for BronzePipeline.

These tests verify that the pipeline orchestrates the Bronze components in a
fixed order and returns the writer's original return value without adding any
additional behavior.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from include.core.exceptions import BronzePipelineException
from include.framework.bronze.pipeline import BronzePipeline


class TestBronzePipeline(unittest.TestCase):
    """Verification for BronzePipeline's DI orchestration contract."""

    def test_run_calls_components_in_order_and_returns_writer_result(self) -> None:
        reader = MagicMock()
        validator = MagicMock()
        metadata = MagicMock()
        writer = MagicMock()
        writer.write.return_value = "/tmp/output"

        reader.read.return_value = "dataframe"
        validator.validate.return_value = "validated_dataframe"
        metadata.enrich.return_value = "enriched_dataframe"

        pipeline = BronzePipeline(reader=reader, validator=validator, metadata=metadata, writer=writer)
        result = pipeline.run(
            data={"symbol": "AAPL"},
            output_path="/tmp/output",
            mode="append",
            format="parquet",
        )

        self.assertEqual(result, "/tmp/output")
        reader.read.assert_called_once_with({"symbol": "AAPL"})
        validator.validate.assert_called_once_with("dataframe")
        metadata.enrich.assert_called_once_with("validated_dataframe")
        writer.write.assert_called_once_with(
            "enriched_dataframe",
            "/tmp/output",
            mode="append",
            format="parquet",
        )

    def test_run_wraps_component_exception(self) -> None:
        reader = MagicMock()
        validator = MagicMock()
        metadata = MagicMock()
        writer = MagicMock()

        reader.read.side_effect = RuntimeError("boom")
        pipeline = BronzePipeline(reader=reader, validator=validator, metadata=metadata, writer=writer)

        with self.assertRaises(BronzePipelineException):
            pipeline.run(data={"symbol": "AAPL"}, output_path="/tmp/output")


if __name__ == "__main__":
    unittest.main()
