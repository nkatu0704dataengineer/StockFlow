"""
Unit tests for FinnhubProvider.
"""

import sys
import types
import unittest
from unittest.mock import MagicMock, patch


requests_stub = types.ModuleType("requests")
requests_stub.Response = object


class HTTPError(Exception):
    """Minimal HTTP error dependency used by provider tests."""


class RequestsExceptions(types.SimpleNamespace):
    """Namespace for requests exception types."""


requests_stub.exceptions = RequestsExceptions(HTTPError=HTTPError)
requests_stub.get = MagicMock()
sys.modules.setdefault("requests", requests_stub)


airflow_stub = types.ModuleType("airflow")
hooks_stub = types.ModuleType("airflow.hooks")
base_stub = types.ModuleType("airflow.hooks.base")


class BaseHook:
    """Minimal Airflow BaseHook stub for provider import-time compatibility."""

    @staticmethod
    def get_connection(conn_id: str):
        raise NotImplementedError


base_stub.BaseHook = BaseHook
sys.modules.setdefault("airflow", airflow_stub)
sys.modules.setdefault("airflow.hooks", hooks_stub)
sys.modules.setdefault("airflow.hooks.base", base_stub)

import requests

from include.api.providers.finnhub import FinnhubProvider
from include.core.exceptions import FinnhubProviderException


class TestFinnhubProvider(unittest.TestCase):
    """Regression tests for production-quality Finnhub provider validation."""

    def setUp(self):
        self.provider = FinnhubProvider(conn_id="test_finnhub_conn")

    @patch("include.api.providers.finnhub.BaseHook.get_connection")
    @patch("include.api.providers.finnhub.requests.get")
    def test_health_check_uses_lightweight_quote_probe(self, mock_get, mock_get_connection):
        """Health check should reuse the quote endpoint with a lightweight symbol probe."""
        mock_conn = MagicMock()
        mock_conn.host = "https://finnhub.example.com"
        mock_conn.login = None
        mock_conn.password = "test_api_key"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        mock_response = MagicMock()
        mock_response.text = '{"c": 120.0, "h": 121.0, "l": 119.0, "o": 118.5, "pc": 117.0, "t": 1718827200}'
        mock_response.json.return_value = {
            "c": 120.0,
            "h": 121.0,
            "l": 119.0,
            "o": 118.5,
            "pc": 117.0,
            "t": 1718827200,
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        self.assertTrue(self.provider.health_check())
        call_args = mock_get.call_args
        called_url = call_args.args[0] if call_args.args else None
        called_kwargs = call_args.kwargs if hasattr(call_args, "kwargs") else {}
        called_params = called_kwargs.get("params")
        self.assertIn("/api/v1/quote", str(called_url))
        self.assertEqual(called_params["symbol"], "AAPL")
        self.assertEqual(called_params["token"], "test_api_key")

    def test_validate_response_rejects_zeroed_quote_payload(self):
        """Quote payloads that consist entirely of zero-valued fields must be rejected."""
        mock_response = MagicMock()
        mock_response.text = '{"c":0, "h":0, "l":0, "o":0, "pc":0, "t":0}'
        mock_response.json.return_value = {
            "c": 0,
            "h": 0,
            "l": 0,
            "o": 0,
            "pc": 0,
            "t": 0,
        }
        mock_response.raise_for_status.return_value = None

        with self.assertRaises(FinnhubProviderException):
            self.provider._validate_response(mock_response)


if __name__ == "__main__":
    unittest.main()
