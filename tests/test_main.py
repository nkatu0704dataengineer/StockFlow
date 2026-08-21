"""Regression tests for the StockFlow composition root.

These tests verify that the application entry point assembles the Bronze
stack in the correct dependency order and executes the Bronze job through the
application-level composition root.
"""

from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock, patch


requests_stub = types.ModuleType("requests")
requests_stub.Response = object


class HTTPError(Exception):
    """Minimal HTTPError dependency used by the provider import surface."""


class RequestsExceptions(types.SimpleNamespace):
    """Namespace for requests exception types."""


requests_stub.exceptions = RequestsExceptions(HTTPError=HTTPError)
requests_stub.get = MagicMock()
sys.modules.setdefault("requests", requests_stub)


airflow_stub = types.ModuleType("airflow")
hooks_stub = types.ModuleType("airflow.hooks")
base_stub = types.ModuleType("airflow.hooks.base")


class BaseHook:
    """Minimal Airflow BaseHook stub for import-time compatibility."""

    @staticmethod
    def get_connection(conn_id: str):
        raise NotImplementedError


base_stub.BaseHook = BaseHook
sys.modules.setdefault("airflow", airflow_stub)
sys.modules.setdefault("airflow.hooks", hooks_stub)
sys.modules.setdefault("airflow.hooks.base", base_stub)

import main


class TestMainCompositionRoot(unittest.TestCase):
    """Verification for the application composition root."""

    @patch("main.BronzeJob")
    @patch("main.BronzePipeline")
    @patch("main.BronzeWriter")
    @patch("main.BronzeMetadata")
    @patch("main.BronzeValidator")
    @patch("main.BronzeReader")
    @patch("main.StockNormalizer")
    @patch("main.ProviderAdapter")
    @patch("main.FinnhubProvider")
    @patch("main._build_spark_session")
    def test_main_composes_and_runs_bronze_job(
        self,
        mock_build_spark_session,
        mock_finnhub_provider,
        mock_provider_adapter,
        mock_stock_normalizer,
        mock_bronze_reader,
        mock_bronze_validator,
        mock_bronze_metadata,
        mock_bronze_writer,
        mock_bronze_pipeline,
        mock_bronze_job,
    ) -> None:
        mock_build_spark_session.return_value = MagicMock()
        mock_provider_adapter.return_value = MagicMock()
        mock_stock_normalizer.return_value = MagicMock()
        mock_bronze_reader.return_value = MagicMock()
        mock_bronze_validator.return_value = MagicMock()
        mock_bronze_metadata.return_value = MagicMock()
        mock_bronze_writer.return_value = MagicMock()
        mock_bronze_pipeline.return_value = MagicMock()
        mock_bronze_job.return_value = MagicMock()

        result = main.main(output_path="/tmp/bronze")

        self.assertEqual(result, 0)
        mock_build_spark_session.assert_called_once()
        mock_finnhub_provider.assert_called_once_with(conn_id="finnhub")
        mock_provider_adapter.assert_called_once()
        mock_stock_normalizer.assert_called_once()
        mock_bronze_reader.assert_called_once()
        mock_bronze_validator.assert_called_once()
        mock_bronze_metadata.assert_called_once()
        mock_bronze_writer.assert_called_once()
        mock_bronze_pipeline.assert_called_once()
        mock_bronze_job.assert_called_once()
        mock_bronze_job.return_value.run.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
