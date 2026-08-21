"""Regression tests for BronzeJob.

These tests verify that BronzeJob reads symbols from SymbolRepository,
invokes the API layer per symbol, delegates to the Bronze pipeline, and
continues processing if one symbol fails.
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from include.core.exceptions import BronzeJobException
from include.framework.bronze.pipeline import BronzePipeline
from include.jobs.bronze.bronze_job import BronzeJob
from include.jobs.repository.symbol_repository import SymbolRepository


class TestBronzeJob(unittest.TestCase):
    """Verification for BronzeJob's orchestration contract."""

    def test_run_uses_symbol_repository_and_pipeline_for_each_symbol(self) -> None:
        api_client = MagicMock()
        pipeline = MagicMock()
        pipeline.run.return_value = "/tmp/output"
        api_client.fetch.side_effect = [
            {"symbol": "AAPL"},
            {"symbol": "MSFT"},
        ]

        job = BronzeJob(api_client=api_client, pipeline=pipeline, output_path="/tmp/output")

        with patch.object(SymbolRepository, "get_all", return_value=["AAPL", "MSFT"]):
            result = job.run()

        self.assertIsNone(result)
        self.assertEqual(api_client.fetch.call_count, 2)
        self.assertEqual(pipeline.run.call_count, 2)
        self.assertEqual(pipeline.run.call_args_list[0].kwargs["data"], {"symbol": "AAPL"})
        self.assertEqual(pipeline.run.call_args_list[1].kwargs["data"], {"symbol": "MSFT"})
        self.assertEqual(pipeline.run.call_args_list[0].kwargs["output_path"], "/tmp/output")

    def test_run_continues_after_symbol_failure(self) -> None:
        api_client = MagicMock()
        pipeline = MagicMock()
        api_client.fetch.side_effect = [RuntimeError("boom"), {"symbol": "TSLA"}]

        job = BronzeJob(api_client=api_client, pipeline=pipeline, output_path="/tmp/output")

        with patch.object(SymbolRepository, "get_all", return_value=["NVDA", "TSLA"]):
            job.run()

        self.assertEqual(api_client.fetch.call_count, 2)
        self.assertEqual(pipeline.run.call_count, 1)
        self.assertEqual(pipeline.run.call_args.kwargs["data"], {"symbol": "TSLA"})

    def test_run_continues_when_pipeline_for_one_symbol_fails(self) -> None:
        api_client = MagicMock()
        pipeline = MagicMock()
        api_client.fetch.side_effect = [
            {"symbol": "AAPL"},
            {"symbol": "TSLA"},
        ]
        pipeline.run.side_effect = [RuntimeError("pipeline boom"), "/tmp/output"]

        job = BronzeJob(api_client=api_client, pipeline=pipeline, output_path="/tmp/output")

        with patch.object(SymbolRepository, "get_all", return_value=["AAPL", "TSLA"]):
            job.run()

        self.assertEqual(api_client.fetch.call_count, 2)
        self.assertEqual(pipeline.run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
