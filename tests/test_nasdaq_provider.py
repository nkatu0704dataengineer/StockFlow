"""
Unit tests for NasdaqProvider.
"""

import unittest
from unittest.mock import MagicMock, patch
import requests

from include.api.providers.nasdaq import NasdaqProvider
from include.core.exceptions import NasdaqProviderException


class TestNasdaqProvider(unittest.TestCase):
    def setUp(self):
        self.conn_id = "test_nasdaq_conn"
        self.provider = NasdaqProvider(conn_id=self.conn_id)

    def test_init(self):
        """Test initialization sets up attributes correctly."""
        self.assertEqual(self.provider._conn_id, self.conn_id)
        self.assertEqual(self.provider.provider_name, "NASDAQ")
        self.assertIsNone(self.provider._host)
        self.assertIsNone(self.provider._login)
        self.assertIsNone(self.provider._password)
        self.assertIsNone(self.provider._extra)
        self.assertIsNone(self.provider._api_key)

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    def test_get_connection_success(self, mock_get_connection):
        """Test _get_connection correctly reads credentials from Airflow Connection."""
        mock_conn = MagicMock()
        mock_conn.host = "https://test.api.nasdaq.com"
        mock_conn.login = "test_user"
        mock_conn.password = "test_password_api_key"
        mock_conn.extra_dejson = {"endpoint": "api/v3/datasets/WIKI/{symbol}/data.json"}
        mock_get_connection.return_value = mock_conn

        self.provider._get_connection()

        self.assertEqual(self.provider._host, "https://test.api.nasdaq.com")
        self.assertEqual(self.provider._login, "test_user")
        self.assertEqual(self.provider._password, "test_password_api_key")
        self.assertEqual(self.provider._extra, {"endpoint": "api/v3/datasets/WIKI/{symbol}/data.json"})
        self.assertEqual(self.provider._api_key, "test_password_api_key")

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    def test_get_connection_api_key_from_extra(self, mock_get_connection):
        """Test that API Key is extracted from extra_dejson if password is not set."""
        mock_conn = MagicMock()
        mock_conn.host = "https://test.api.nasdaq.com"
        mock_conn.login = None
        mock_conn.password = None
        mock_conn.extra_dejson = {"api_key": "extra_api_key"}
        mock_get_connection.return_value = mock_conn

        self.provider._get_connection()
        self.assertEqual(self.provider._api_key, "extra_api_key")

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    def test_get_connection_missing_host(self, mock_get_connection):
        """Test that _get_connection raises NasdaqProviderException if host is missing."""
        mock_conn = MagicMock()
        mock_conn.host = None
        mock_conn.login = "test"
        mock_conn.password = "key"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        with self.assertRaises(NasdaqProviderException) as context:
            self.provider._get_connection()
        self.assertIn("missing the 'host' attribute", str(context.exception))

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    def test_get_connection_missing_api_key(self, mock_get_connection):
        """Test that _get_connection raises NasdaqProviderException if API key is missing."""
        mock_conn = MagicMock()
        mock_conn.host = "https://test.api.nasdaq.com"
        mock_conn.login = None
        mock_conn.password = None
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        with self.assertRaises(NasdaqProviderException) as context:
            self.provider._get_connection()
        self.assertIn("missing API key", str(context.exception))

    def test_build_url_default(self):
        """Test build_url construction with default template."""
        self.provider._host = "https://test.api.nasdaq.com/"
        self.provider._extra = {}
        
        url = self.provider._build_url("AAPL")
        self.assertEqual(url, "https://test.api.nasdaq.com/api/v3/datasets/WIKI/AAPL/data.json")

    def test_build_url_custom_endpoint_with_placeholder(self):
        """Test build_url with custom endpoint that includes a {symbol} placeholder."""
        self.provider._host = "https://test.api.nasdaq.com"
        self.provider._extra = {"endpoint": "custom/v1/{symbol}.json"}

        url = self.provider._build_url("MSFT")
        self.assertEqual(url, "https://test.api.nasdaq.com/custom/v1/MSFT.json")

    def test_build_url_custom_endpoint_no_placeholder(self):
        """Test build_url with custom endpoint without placeholder."""
        self.provider._host = "https://test.api.nasdaq.com"
        self.provider._extra = {"endpoint": "custom/v1/fetch"}

        url = self.provider._build_url("TSLA")
        self.assertEqual(url, "https://test.api.nasdaq.com/custom/v1/fetch/TSLA")

    def test_build_headers(self):
        """Test _build_headers correctly merges authorization and extra headers."""
        self.provider._api_key = "my_api_key_123"
        self.provider._extra = {
            "headers": {
                "custom-header": "custom-value",
                "Content-Type": "application/xml"  # Should overwrite base content-type if desired
            }
        }

        headers = self.provider._build_headers()
        self.assertEqual(headers["Accept"], "application/json")
        self.assertEqual(headers["Content-Type"], "application/xml")
        self.assertEqual(headers["Authorization"], "Bearer my_api_key_123")
        self.assertEqual(headers["x-api-key"], "my_api_key_123")
        self.assertEqual(headers["custom-header"], "custom-value")

    @patch("include.api.providers.nasdaq.requests.get")
    def test_send_request(self, mock_get):
        """Test _send_request calls requests.get with correct timeout."""
        self.provider._extra = {"timeout": 15.5}
        url = "https://example.com"
        headers = {"Accept": "application/json"}
        
        self.provider._send_request(url, headers)
        mock_get.assert_called_once_with(url, headers=headers, timeout=15.5)

    def test_validate_response_success(self):
        """Test _validate_response parses valid JSON correctly."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.text = '{"dataset": "test_data"}'
        mock_response.json.return_value = {"dataset": "test_data"}
        
        data = self.provider._validate_response(mock_response)
        self.assertEqual(data, {"dataset": "test_data"})
        mock_response.raise_for_status.assert_called_once()

    def test_validate_response_http_error(self):
        """Test _validate_response raises NasdaqProviderException on HTTP errors."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")

        with self.assertRaises(NasdaqProviderException) as context:
            self.provider._validate_response(mock_response)
        self.assertIn("HTTP error response from NASDAQ API", str(context.exception))

    def test_validate_response_empty(self):
        """Test _validate_response raises NasdaqProviderException on empty responses."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.text = "   "
        
        with self.assertRaises(NasdaqProviderException) as context:
            self.provider._validate_response(mock_response)
        self.assertIn("Received empty response", str(context.exception))

    def test_validate_response_invalid_json(self):
        """Test _validate_response raises NasdaqProviderException on JSON parsing errors."""
        mock_response = MagicMock(spec=requests.Response)
        mock_response.text = "invalid json content"
        mock_response.json.side_effect = ValueError("JSON decode error")

        with self.assertRaises(NasdaqProviderException) as context:
            self.provider._validate_response(mock_response)
        self.assertIn("Failed to parse JSON response", str(context.exception))

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    @patch("include.api.providers.nasdaq.requests.get")
    def test_fetch_success(self, mock_get, mock_get_connection):
        """Test the full fetch workflow works end-to-end under successful conditions."""
        # Setup Connection mock
        mock_conn = MagicMock()
        mock_conn.host = "https://test.api.nasdaq.com"
        mock_conn.password = "api_key"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        # Setup requests mock
        mock_response = MagicMock()
        mock_response.text = '{"success": true}'
        mock_response.json.return_value = {"success": True}
        mock_get.return_value = mock_response

        # Execute
        result = self.provider.fetch("NVDA")

        # Verify
        self.assertEqual(result, {"success": True})
        mock_get_connection.assert_called_once_with(self.conn_id)
        mock_get.assert_called_once()

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    @patch("include.api.providers.nasdaq.requests.get")
    def test_fetch_unexpected_error_wrapped(self, mock_get, mock_get_connection):
        """Test that unexpected exceptions in fetch are wrapped in NasdaqProviderException."""
        mock_conn = MagicMock()
        mock_conn.host = "https://test.api.nasdaq.com"
        mock_conn.password = "api_key"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        # Mock request to raise connection error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection timed out")

        with self.assertRaises(NasdaqProviderException) as context:
            self.provider.fetch("NVDA")
        self.assertIn("Unexpected error fetching data for symbol NVDA", str(context.exception))

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    @patch("include.api.providers.nasdaq.requests.get")
    def test_health_check_success(self, mock_get, mock_get_connection):
        """Test health check returns True on success."""
        mock_conn = MagicMock()
        mock_conn.host = "https://test.api.nasdaq.com"
        mock_conn.password = "api_key"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        self.assertTrue(self.provider.health_check())

    @patch("include.api.providers.nasdaq.BaseHook.get_connection")
    @patch("include.api.providers.nasdaq.requests.get")
    def test_health_check_failure(self, mock_get, mock_get_connection):
        """Test health check returns False on server errors or exceptions."""
        mock_conn = MagicMock()
        mock_conn.host = "https://test.api.nasdaq.com"
        mock_conn.password = "api_key"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        # Mock server error (500)
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        self.assertFalse(self.provider.health_check())

        # Mock connection failure
        mock_get.side_effect = Exception("Network down")
        self.assertFalse(self.provider.health_check())


if __name__ == "__main__":
    unittest.main()
