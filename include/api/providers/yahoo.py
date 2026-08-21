"""
Yahoo Finance API Client integration.

This module implements the YahooProvider which inherits from BaseProvider.
It follows the reference implementation established by NasdaqProvider and FinnhubProvider.

Notes
-----
Yahoo Finance exposes a public REST API at ``query1.finance.yahoo.com``.
This provider uses the v8 chart endpoint to retrieve OHLCV market data
for a given symbol without requiring an API key.

Authentication is not required, but the Airflow Connection is still used to
centralise all configurable parameters (host, range, interval, timeout, and
any additional request headers) consistent with the StockFlow convention.
"""

from __future__ import annotations

import logging
from typing import Any
import requests

from airflow.hooks.base import BaseHook

from include.api.providers.base import BaseProvider
from include.core.exceptions import YahooProviderException


class YahooProvider(BaseProvider):
    """
    Yahoo Finance data provider.

    This provider is responsible ONLY for communicating with the Yahoo Finance
    REST API, utilizing Airflow Connections for host and configurable
    fetch parameters.

    Notes
    -----
    Yahoo Finance does not require an API key for basic market data.
    The Airflow Connection is still used to centralise all configuration
    (host, range, interval, timeout, headers) via the ``extra`` field,
    keeping the provider fully consistent with the StockFlow convention.
    """

    def __init__(self, conn_id: str) -> None:
        """
        Initialize the YahooProvider.

        Parameters
        ----------
        conn_id : str
            The Airflow connection ID to retrieve host and fetch configuration.
        """
        self._conn_id = conn_id
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Connection attributes populated during _get_connection()
        self._host: str | None = None
        self._login: str | None = None
        self._password: str | None = None
        self._extra: dict[str, Any] | None = None

    @property
    def provider_name(self) -> str:
        """
        Human-readable provider name.

        Returns
        -------
        str
            "Yahoo Finance"
        """
        return "Yahoo Finance"

    def _get_connection(self) -> None:
        """
        Read the Airflow Connection and extract host and fetch configuration.

        This method reads the connection using Airflow's BaseHook and extracts
        the host and any configurable fetch parameters stored in the ``extra``
        field (range, interval, timeout, headers).

        Raises
        ------
        YahooProviderException
            If the connection cannot be retrieved, the extra field cannot be
            parsed as JSON, or the host attribute is missing.
        """
        self.logger.info(f"Retrieving Airflow connection details for ID: {self._conn_id}")
        try:
            connection = BaseHook.get_connection(self._conn_id)
        except Exception as e:
            raise YahooProviderException(
                f"Failed to retrieve Airflow connection '{self._conn_id}': {str(e)}"
            ) from e

        self._host = connection.host
        self._login = connection.login
        self._password = connection.password

        try:
            self._extra = connection.extra_dejson or {}
        except Exception as e:
            raise YahooProviderException(
                f"Failed to parse connection 'extra' field as JSON: {str(e)}"
            ) from e

        if not self._host:
            raise YahooProviderException(
                f"Airflow connection '{self._conn_id}' is missing the 'host' attribute."
            )

    def _build_url(self, symbol: str) -> str:
        """
        Build the Yahoo Finance chart endpoint path for the given symbol.

        This method is responsible only for constructing the base endpoint
        path. Query parameters are handled separately by ``_build_params()``
        and forwarded via the ``params=`` argument of ``requests.get()``.

        Parameters
        ----------
        symbol : str
            Stock ticker symbol (already normalized).

        Returns
        -------
        str
            The base endpoint URL without any query parameters.

        Raises
        ------
        YahooProviderException
            If host is not set (i.e. _get_connection was not run).
        """
        if not self._host:
            raise YahooProviderException("Host is not set. Did you run _get_connection() first?")

        base_url = self._host.rstrip("/")
        endpoint = self._extra.get("endpoint") if self._extra else None

        if endpoint:
            endpoint_str = str(endpoint).strip().lstrip("/")
            if "{symbol}" in endpoint_str:
                path = endpoint_str.format(symbol=symbol)
            else:
                path = f"{endpoint_str.rstrip('/')}/{symbol}"
        else:
            # Default Yahoo Finance v8 chart endpoint
            path = f"v8/finance/chart/{symbol}"

        url = f"{base_url}/{path}"
        self.logger.debug(f"Constructed base URL: {url}")
        return url

    def _build_params(self) -> dict[str, str]:
        """
        Build the query parameter dictionary for the Yahoo Finance request.

        Reads ``range`` and ``interval`` from the connection extra, falling
        back to sensible defaults. Using ``params=`` in ``requests.get()``
        instead of manual string concatenation ensures proper URL encoding.

        Returns
        -------
        dict[str, str]
            Query parameters forwarded to ``requests.get()``.
        """
        params = {
            "range": self._extra.get("range", "1d") if self._extra else "1d",
            "interval": self._extra.get("interval", "1d") if self._extra else "1m",
        }
        self.logger.debug(f"Constructed request params: {params}")
        return params

    def _build_headers(self) -> dict[str, str]:
        """
        Create request headers for the Yahoo Finance API.

        Yahoo Finance does not require an Authorization header. A browser-like
        ``User-Agent`` is included to avoid request rejections by the public
        endpoint. Any additional headers defined in the connection extra are
        merged on top.

        Returns
        -------
        dict[str, str]
            Request headers.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            # Yahoo Finance's public API rejects requests without a User-Agent
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

        # Merge with any additional headers defined in the connection extra
        if self._extra and "headers" in self._extra:
            extra_headers = self._extra["headers"]
            if isinstance(extra_headers, dict):
                headers.update({str(k): str(v) for k, v in extra_headers.items()})

        return headers

    def _send_request(
        self,
        url: str,
        headers: dict[str, str],
        params: dict[str, str],
    ) -> requests.Response:
        """
        Execute HTTP GET request.

        Parameters
        ----------
        url : str
            The base endpoint URL (no query string).
        headers : dict[str, str]
            HTTP headers to include.
        params : dict[str, str]
            Query parameters forwarded via ``requests.get(params=...)``.

        Returns
        -------
        requests.Response
            The raw HTTP response.
        """
        timeout = 10.0
        if self._extra and "timeout" in self._extra:
            try:
                timeout = float(self._extra["timeout"])
            except (ValueError, TypeError):
                self.logger.warning(
                    "Invalid timeout value in connection extra. Using default of 10s."
                )

        self.logger.debug(f"Sending GET request to {url} with params={params}, timeout={timeout}s")
        return requests.get(url, headers=headers, params=params, timeout=timeout)

    def _validate_response(self, response: requests.Response) -> Any:
        """
        Perform basic HTTP, JSON, and business-level validation on the response.

        Parameters
        ----------
        response : requests.Response
            The HTTP response to validate.

        Returns
        -------
        Any
            Parsed raw JSON data.

        Raises
        ------
        YahooProviderException
            If status code is not 2xx, response is empty, JSON parsing fails,
            or the Yahoo Finance payload contains a business-level error.
        """
        # 1. Validate HTTP Status Code
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise YahooProviderException(
                f"HTTP error response from Yahoo Finance API: "
                f"{response.status_code} - {response.text}"
            ) from e

        # 2. Validate Empty Response
        text = response.text
        if not text or not text.strip():
            raise YahooProviderException("Received empty response from Yahoo Finance API.")

        # 3. Validate JSON Parsing
        try:
            data = response.json()
        except ValueError as e:
            raise YahooProviderException(
                f"Failed to parse JSON response from Yahoo Finance API: {str(e)}"
            ) from e

        # 4. Validate Yahoo Finance business-level error
        # Yahoo Finance embeds errors inside a 200 OK payload under chart.error.
        chart_error = data.get("chart", {}).get("error")
        if chart_error:
            code = chart_error.get("code", "UNKNOWN")
            description = chart_error.get("description", str(chart_error))
            raise YahooProviderException(
                f"Yahoo Finance API returned a business error: [{code}] {description}"
            )

        return data

    def fetch(self, symbol: str) -> Any:
        """
        Fetch raw market data for a given symbol from Yahoo Finance.

        This method orchestrates the entire workflow:
        _get_connection() -> _build_url() -> _build_params() -> _build_headers()
        -> _send_request() -> _validate_response()

        The input symbol is normalized (stripped and uppercased) before any
        request is constructed.

        Parameters
        ----------
        symbol : str
            Stock ticker.

        Returns
        -------
        Any
            Raw JSON response returned by the Yahoo Finance API.

        Raises
        ------
        YahooProviderException
            If the request fails for any reason.
        """
        symbol = symbol.strip().upper()
        self.logger.info(f"Starting fetch workflow for symbol: {symbol}")
        try:
            self._get_connection()
            url = self._build_url(symbol)
            params = self._build_params()
            headers = self._build_headers()
            response = self._send_request(url, headers, params)
            data = self._validate_response(response)
            self.logger.info(f"Successfully fetched Yahoo Finance data for symbol: {symbol}")
            return data
        except YahooProviderException:
            raise
        except Exception as e:
            raise YahooProviderException(
                f"Unexpected error fetching data for symbol {symbol}: {str(e)}"
            ) from e

    def health_check(self) -> bool:
        """
        Check whether Yahoo Finance is currently reachable.

        Probes the chart endpoint for a well-known symbol (``AAPL``) using a
        minimal 1-day range to confirm API reachability without incurring a
        heavy data transfer. Reuses ``_validate_response()`` so the health
        check is subject to the same validation rules as a normal fetch.

        Returns
        -------
        bool
            True if Yahoo Finance is reachable and returns valid data.
            False otherwise.
        """
        self.logger.info("Performing health check for Yahoo Finance provider")
        try:
            self._get_connection()
            url = self._build_url("AAPL")
            params = self._build_params()
            headers = self._build_headers()
            response = self._send_request(url, headers, params)
            self._validate_response(response)
            return True
        except Exception as e:
            self.logger.warning(f"Health check failed for Yahoo Finance provider: {str(e)}")
            return False
