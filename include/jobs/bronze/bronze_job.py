"""
Bronze job entry point.

This module implements BronzeJob, the top-level runnable orchestration job for
Bronze ingestion. It reads symbols from SymbolRepository, fetches quotes via
an injected API client, and delegates each normalized quote to the injected
Bronze pipeline. It intentionally performs no Spark or business handling.
"""

from __future__ import annotations

import logging
from typing import Any

from include.core.exceptions import BronzeJobException
from include.helpers.storage_path_builder import StoragePathBuilder
from include.jobs.repository.symbol_repository import SymbolRepository


class BronzeJob:
    """
    Orchestrate Bronze ingestion using dependency injection.

    Parameters
    ----------
    api_client : Any
        API client dependency used to fetch a quote for a ticker symbol.
    pipeline : Any
        Bronze pipeline dependency that sequences reader/validator/metadata/writer.
    logger : logging.Logger | None, optional
        Optional logger used for job diagnostics.
    """

    def __init__(
        self,
        api_client: Any,
        pipeline: Any,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the BronzeJob with injected dependencies.

        Parameters
        ----------
        api_client : Any
            API layer client used to fetch raw quote data.
        pipeline : Any
            Bronze pipeline used to process a single quote.
        logger : logging.Logger | None, optional
            Optional logger for Bronze job diagnostics.
        """
        self.api_client = api_client
        self.pipeline = pipeline
        self.logger = logger or logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def run(self) -> None:
        """
        Execute the Bronze job workflow.

        The job resolves all configured symbols from ``SymbolRepository``, fetches
        a quote for each symbol through the injected API client, and forwards the
        resulting quote to the injected Bronze pipeline. Per-symbol failures are
        logged and skipped so the job as a whole can continue processing the
        remaining symbols.

        Returns
        -------
        None
            This job is an orchestration entry point and does not return a
            business object.

        Raises
        ------
        BronzeJobException
            If the overall job orchestrator fails unexpectedly.
        """
        self.logger.info("job_started")

        symbols = SymbolRepository.get_all()
        failed_symbols: list[str] = []

        try:
            for symbol in symbols:
                self.logger.info("processing_symbol", extra={"symbol": symbol})
                try:
                    quote = self.api_client.fetch(symbol)
                    self.pipeline.run(
                        data=quote,
                        output_path = StoragePathBuilder.build_bronze_path(quote),
                    )
                    self.logger.info("symbol_completed", extra={"symbol": symbol})
                except Exception as exc:
                    failed_symbols.append(symbol)
                    self.logger.exception(
                        "symbol_failed",
                        extra={"symbol": symbol, "error": str(exc)},
                    )
                    continue
        except Exception as exc:
            self.logger.warning("job_failed", extra={"error": str(exc)})
            raise BronzeJobException("Bronze job execution failed.") from exc

        self.logger.info(
            "job_finished",
            extra={"processed_symbols": len(symbols) - len(failed_symbols), "failed_symbols": failed_symbols},
        )
