"""
Finnhub API Client integration.

This module implements the FinnhubProvider which inherits from BaseProvider.
It follows the reference implementation established by NasdaqProvider.
"""

from __future__ import annotations

import logging
from typing import Any
import requests

from airflow.hooks.base import BaseHook

from include.api.providers.base import BaseProvider
from include.core.exceptions import FinnhubProviderException


class FinnhubProvider(BaseProvider):
    """
    Finnhub data provider.

    This provider is responsible ONLY for communicating with the Finnhub API,
    utilizing Airflow Connections for authentication and connection details.

    Notes
    -----
    Finnhub authenticates via an API token passed as a query parameter
    (``token=<api_key>``), not via a Bearer Authorization header.
    The token and symbol are forwarded via the ``params=`` argument of
    ``requests.get()`` by ``_build_params()``.
    """

    def __init__(self, conn_id: str) -> None:
        """
        Initialize the FinnhubProvider.

        Parameters
        ----------
        conn_id : str
            The Airflow connection ID to retrieve credentials and host.
        """
        self._conn_id = conn_id
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Connection attributes populated during _get_connection()
        self._host: str | None = None
        self._login: str | None = None
        self._password: str | None = None
        self._extra: dict[str, Any] | None = None
        self._api_key: str | None = None

    @property
    def provider_name(self) -> str:
        """
        Human-readable provider name.

        Returns
        -------
        str
            "Finnhub"
        """
        return "Finnhub"

    def _get_connection(self) -> None:
        """
        Read the Airflow Connection and extract credentials.

        This method reads the connection using Airflow's BaseHook and extracts
        host, credentials, extra details, and the API key.

        Raises
        ------
        FinnhubProviderException
            If the connection is missing or cannot be retrieved, or if required
            parameters (host or api key) are missing.
        """
        self.logger.info(f"Retrieving Airflow connection details for ID: {self._conn_id}")
        try:
            connection = BaseHook.get_connection(self._conn_id)
        except Exception as e:
            raise FinnhubProviderException(
                f"Failed to retrieve Airflow connection '{self._conn_id}': {str(e)}"
            ) from e

        self._host = connection.host
        self._login = connection.login
        self._password = connection.password

        try:
            self._extra = connection.extra_dejson or {}
        except Exception as e:
            raise FinnhubProviderException(
                f"Failed to parse connection 'extra' field as JSON: {str(e)}"
            ) from e

        # Extract the API key from various possible connection parameters
        self._api_key = (
            self._password
            or self._extra.get("api_key")
            or self._extra.get("apiKey")
            or self._extra.get("api-key")
            or self._login
        )

        if not self._host:
            raise FinnhubProviderException(
                f"Airflow connection '{self._conn_id}' is missing the 'host' attribute."
            )

        if not self._api_key:
            raise FinnhubProviderException(
                f"Airflow connection '{self._conn_id}' is missing API key / credentials."
            )

    def _build_url(self, symbol: str) -> str:
        """
        Build the Finnhub endpoint path for the given symbol.

        This method is responsible only for constructing the base endpoint
        path. Query parameters (symbol and token) are handled separately by
        ``_build_params()`` and forwarded via the ``params=`` argument of
        ``requests.get()``.

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
        FinnhubProviderException
            If host is not set (i.e. _get_connection was not run).
        """
        if not self._host:
            raise FinnhubProviderException("Host is not set. Did you run _get_connection() first?")

        base_url = self._host.rstrip("/")
        endpoint = self._extra.get("endpoint") if self._extra else None

        if endpoint:
            endpoint_str = str(endpoint).strip().lstrip("/")
            if "{symbol}" in endpoint_str:
                path = endpoint_str.format(symbol=symbol)
            else:
                path = endpoint_str.rstrip("/")
        else:
            # Default Finnhub real-time quote endpoint
            path = "api/v1/quote"

        url = f"{base_url}/{path}"
        self.logger.debug(f"Constructed base URL: {url}")
        return url

    def _build_params(self, symbol: str) -> dict[str, str]:
        """
        Build the query parameter dictionary for the Finnhub request.

        Finnhub requires both the ``symbol`` and the auth ``token`` as query
        parameters. Using ``params=`` in ``requests.get()`` instead of manual
        string concatenation ensures proper URL encoding.

        Parameters
        ----------
        symbol : str
            Stock ticker symbol (already normalized).

        Returns
        -------
        dict[str, str]
            Query parameters forwarded to ``requests.get()``.
        """
        params = {
            "symbol": symbol,
            "token": self._api_key,
        }
        self.logger.debug(f"Constructed request params: {params}")
        return params

    def _build_headers(self) -> dict[str, str]:
        """
        Create request headers for the Finnhub API.

        Finnhub does not require an Authorization header — authentication is
        handled via the ``token`` query parameter in the URL. This method
        sets standard Accept / Content-Type headers and merges any additional
        headers defined in the connection extra.

        Returns
        -------
        dict[str, str]
            Request headers.
        """
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
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
                self.logger.warning("Invalid timeout value in connection extra. Using default of 10s.")

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
        FinnhubProviderException
            If status code is not 2xx, response is empty, JSON parsing fails,
            or the Finnhub payload contains a business-level error.
        """
        # 1. Validate HTTP Status Code
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise FinnhubProviderException(
                f"HTTP error response from Finnhub API: {response.status_code} - {response.text}"
            ) from e

        # 2. Validate Empty Response
        text = response.text
        if not text or not text.strip():
            raise FinnhubProviderException("Received empty response from Finnhub API.")

        # 3. Validate JSON Parsing
        try:
            data = response.json()
        except ValueError as e:
            raise FinnhubProviderException(
                f"Failed to parse JSON response from Finnhub API: {str(e)}"
            ) from e

        # 4. Validate Finnhub business-level error
        # Finnhub embeds access or parameter errors as a top-level "error" key.
        api_error = data.get("error") if isinstance(data, dict) else None
        if api_error:
            raise FinnhubProviderException(
                f"Finnhub API returned a business error: {api_error}"
            )

        # 5. Validate basic quote payload meaning before returning.
        # This protects downstream layers from zero-valued or otherwise
        # non-meaningful quote payloads without performing normalization.
        self._validate_quote_payload(data)

        return data

    def _validate_quote_payload(self, payload: Any) -> None:
        """
        Validate that a parsed Finnhub quote payload contains meaningful data.

        Parameters
        ----------
        payload : Any
            Parsed JSON payload from the Finnhub quote endpoint.

        Raises
        ------
        FinnhubProviderException
            If the payload is a quote response but contains no meaningful quote
            signal.
        """
        if not isinstance(payload, dict):
            return

        quote_keys = {"c", "h", "l", "o", "pc", "t"}
        if not quote_keys.intersection(payload.keys()):
            return

        timestamp = payload.get("t")
        current_price = payload.get("c")
        timestamp_is_meaningful = isinstance(timestamp, (int, float)) and timestamp > 0
        current_price_is_meaningful = isinstance(current_price, (int, float)) and current_price != 0

        if not timestamp_is_meaningful and not current_price_is_meaningful:
            raise FinnhubProviderException(
                "Finnhub quote payload is invalid: timestamp and current price are not meaningful."
            )

    def fetch(self, symbol: str) -> Any:
        """
        Fetch raw stock data for a given symbol from Finnhub.

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
            Raw JSON response returned by the provider.

        Raises
        ------
        FinnhubProviderException
            If request fails.
        """
        symbol = symbol.strip().upper()
        self.logger.info(
            "fetch_started",
            extra={
                "provider": self.provider_name,
                "symbol": symbol,
            },
        )
        try:
            self._get_connection()
            url = self._build_url(symbol)
            params = self._build_params(symbol)
            headers = self._build_headers()
            response = self._send_request(url, headers, params)
            data = self._validate_response(response)
            self.logger.info(
                "fetch_succeeded",
                extra={
                    "provider": self.provider_name,
                    "symbol": symbol,
                },
            )
            return data
        except FinnhubProviderException:
            raise
        except Exception as e:
            raise FinnhubProviderException(
                f"Unexpected error fetching data for symbol {symbol}: {str(e)}"
            ) from e

    def health_check(self) -> bool:
        """
        Check whether this provider is currently reachable.

        Uses the lightweight Finnhub quote endpoint with a known symbol probe
        rather than fetching a large market-status payload. Reuses
        ``_send_request()`` and ``_validate_response()`` so the health check is
        subject to the same validation rules as a normal fetch.

        Returns
        -------
        bool
            True if the provider is reachable and returns valid data.
            False otherwise.
        """
        self.logger.info(
            "health_check_started",
            extra={
                "provider": self.provider_name,
                "symbol": "AAPL",
            },
        )
        try:
            self._get_connection()
            base_url = self._host.rstrip("/")
            probe_url = f"{base_url}/api/v1/quote"
            probe_params = {"symbol": "AAPL", "token": self._api_key}
            headers = self._build_headers()
            response = self._send_request(probe_url, headers, probe_params)
            self._validate_response(response)
            self.logger.info(
                "health_check_succeeded",
                extra={
                    "provider": self.provider_name,
                    "symbol": "AAPL",
                },
            )
            return True
        except Exception as e:
            self.logger.warning(
                "health_check_failed",
                extra={
                    "provider": self.provider_name,
                    "symbol": "AAPL",
                    "error_message": str(e),
                },
            )
            return False
