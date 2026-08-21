"""
StockFlow Provider Base Class

Author: StockFlow Team
Project: StockFlow

Description
-----------
This module defines the abstract contract for every stock data provider
used in StockFlow.

Any provider (NASDAQ, Yahoo, Finnhub, Polygon, Alpha Vantage, ...)
MUST inherit from BaseProvider and implement all abstract methods.

Design Principles
-----------------
- Single Responsibility Principle
- Open / Closed Principle
- Dependency Inversion
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseProvider(ABC):
    """
    Abstract base class for every StockFlow provider.

    A provider is responsible ONLY for communicating with
    an external data source.

    Responsibilities
    ----------------
    ✓ Build API request
    ✓ Send request
    ✓ Receive response
    ✓ Return raw response

    NOT Responsible For
    -------------------
    ✗ Retry strategy
    ✗ Fallback logic
    ✗ Data normalization
    ✗ Business validation
    ✗ Spark transformation
    ✗ Storage
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """
        Human-readable provider name.

        Example
        -------
        "NASDAQ"
        "Yahoo Finance"
        "Finnhub"
        """
        raise NotImplementedError

    @abstractmethod
    def fetch(self, symbol: str) -> Any:
        """
        Fetch raw stock data for a given symbol.

        Parameters
        ----------
        symbol : str
            Stock ticker.

        Example
        -------
        NVDA
        AAPL
        TSLA

        Returns
        -------
        Any
            Raw response returned by the provider.
            No transformation should be performed here.

        Raises
        ------
        ProviderException
            If request fails.
        """
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check whether this provider is currently reachable.

        Returns
        -------
        bool
            True if provider is available.
            False otherwise.
        """
        raise NotImplementedError