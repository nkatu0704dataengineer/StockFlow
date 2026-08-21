"""
StockFlow Provider Adapter.

This module implements the API orchestration layer responsible for
executing a single injected provider with retry handling, structured
logging, and exception management.

The adapter is intentionally kept independent from DAG scheduling, Spark,
transformation, storage, or normalization concerns. Those responsibilities
belong to higher layers of the architecture.
"""

from __future__ import annotations

import logging
from typing import Any

from include.api.providers.base import BaseProvider
from include.core.exceptions import ProviderAdapterException


class ProviderAdapter:
    """
    Central execution component for a single provider.

    The adapter hides provider invocation details from callers and provides
    a single entry point for API orchestration.

    Parameters
    ----------
    provider : BaseProvider
        Provider instance to execute.
    max_retry : int, optional
        Maximum number of attempts to perform for the provider. The default
        is ``DEFAULT_MAX_RETRY``.

    Attributes
    ----------
    DEFAULT_MAX_RETRY : int
        Default retry budget for a provider execution.
    """

    DEFAULT_MAX_RETRY = 3

    def __init__(
        self,
        provider: BaseProvider,
        max_retry: int = DEFAULT_MAX_RETRY,
    ) -> None:
        """
        Initialize the provider adapter with a dependency-injected provider.

        Parameters
        ----------
        provider : BaseProvider
            Provider instance to execute inside the adapter.
        max_retry : int, optional
            Number of retry attempts to allow for the provider.

        Raises
        ------
        ProviderAdapterException
            If no provider is supplied or if ``max_retry`` is invalid.
        """
        if provider is None:
            raise ProviderAdapterException("ProviderAdapter requires a provider instance.")

        if max_retry < 1:
            raise ProviderAdapterException("max_retry must be greater than or equal to 1.")

        self._provider = provider
        self._max_retry = max_retry
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def fetch(self, symbol: str) -> Any:
        """
        Fetch raw stock data using the injected provider.

        Parameters
        ----------
        symbol : str
            Stock ticker symbol to fetch.

        Returns
        -------
        Any
            Raw JSON response returned by the provider.

        Raises
        ------
        ProviderAdapterException
            If the provider fails after exhausting the retry budget.
        """
        normalized_symbol = symbol.strip().upper()
        provider_name = self._provider.provider_name

        self.logger.info(
            "fetch_started",
            extra={
                "provider": provider_name,
                "symbol": normalized_symbol,
                "max_retry": self._max_retry,
            },
        )

        try:
            data = self._try_provider(symbol=normalized_symbol)
        except ProviderAdapterException as exc:
            self.logger.error(
                "final_failure",
                extra={
                    "provider": provider_name,
                    "symbol": normalized_symbol,
                    "error_message": str(exc),
                },
            )
            raise

        self.logger.info(
            "final_success",
            extra={
                "provider": provider_name,
                "symbol": normalized_symbol,
            },
        )
        return data

    def health_check(self) -> bool:
        """
        Run the injected provider health check.

        Returns
        -------
        bool
            True if the provider reports it is reachable.
        """
        return bool(self._provider.health_check())

    def _try_provider(self, symbol: str) -> Any:
        """
        Attempt to fetch data from the injected provider with local retry logic.

        Parameters
        ----------
        symbol : str
            Stock ticker symbol.

        Returns
        -------
        Any
            Raw JSON response returned by the provider.

        Raises
        ------
        ProviderAdapterException
            If the provider fails all attempts.
        """
        provider_name = self._provider.provider_name
        last_error_message = "unknown provider failure"

        for attempt in range(1, self._max_retry + 1):
            self.logger.info(
                "retry_started",
                extra={
                    "provider": provider_name,
                    "symbol": symbol,
                    "retry_attempt": attempt,
                    "max_retry": self._max_retry,
                },
            )

            try:
                result = self._provider.fetch(symbol)
            except Exception as exc:
                last_error_message = str(exc)
                self._log_provider_error(
                    provider_name=provider_name,
                    symbol=symbol,
                    attempt=attempt,
                    exception=exc,
                )

                if attempt == self._max_retry:
                    raise ProviderAdapterException(
                        f"{provider_name} failed after {self._max_retry} attempts for "
                        f"symbol '{symbol}': {last_error_message}"
                    ) from exc
                continue

            self.logger.info(
                "retry_succeeded",
                extra={
                    "provider": provider_name,
                    "symbol": symbol,
                    "retry_attempt": attempt,
                },
            )
            return result

        raise ProviderAdapterException(
            f"{provider_name} failed after {self._max_retry} attempts for symbol "
            f"'{symbol}': {last_error_message}"
        )

    def _log_provider_error(
        self,
        provider_name: str,
        symbol: str,
        attempt: int,
        exception: Exception,
    ) -> None:
        """
        Emit structured logging for a provider retry failure event.

        Parameters
        ----------
        provider_name : str
            Provider display name.
        symbol : str
            Stock ticker symbol.
        attempt : int
            Current retry attempt number.
        exception : Exception
            Exception raised by the provider.
        """
        self.logger.exception(
        f"""
        Retry failed.

        Provider : {provider_name}
        Symbol   : {symbol}
        Attempt  : {attempt}

        Exception:
        {exception}
        """
        )
