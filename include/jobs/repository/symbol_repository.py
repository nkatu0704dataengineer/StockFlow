"""
Stock symbol repository.

This module provides the list of stock symbols processed by StockFlow.

The repository is intentionally simple for Bronze v1. It exposes a static
collection of symbols and does not communicate with external systems.

Future versions may load symbols from:

- Database
- Configuration file
- REST API
- Airflow Variables
- Feature Store
"""

from __future__ import annotations


class SymbolRepository:
    """
    Repository containing stock symbols processed by StockFlow.

    Notes
    -----
    Bronze v1 uses a static in-memory collection.

    Future implementations may replace the underlying storage without
    changing the public interface.
    """

    _SYMBOLS = (
        "AAPL",
        "MSFT",
        "GOOGL",
        "NVDA",
        "TSLA",
    )

    @classmethod
    def get_all(cls) -> list[str]:
        """
        Return all configured stock symbols.

        Returns
        -------
        list[str]
            Stock ticker symbols.
        """
        return list(cls._SYMBOLS)

    @classmethod
    def exists(cls, symbol: str) -> bool:
        """
        Check whether a symbol exists in the repository.

        Parameters
        ----------
        symbol : str
            Stock ticker symbol.

        Returns
        -------
        bool
            True if the symbol exists.
        """
        return symbol.strip().upper() in cls._SYMBOLS

    @classmethod
    def count(cls) -> int:
        """
        Return the number of configured symbols.

        Returns
        -------
        int
            Total symbol count.
        """
        return len(cls._SYMBOLS)