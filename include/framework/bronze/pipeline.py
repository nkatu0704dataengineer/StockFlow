"""
Bronze pipeline orchestrator.

This module implements BronzePipeline, the final orchestration component in
Bronze. It coordinates the reader, validator, metadata, and writer in a
strict fixed order without owning any of their internal business logic.
"""

from __future__ import annotations

import logging
from typing import Any

from include.core.exceptions import BronzePipelineException


class BronzePipeline:
    """
    Orchestrate the Bronze processing flow using dependency injection.

    Parameters
    ----------
    reader : Any
        Reader component that converts Python objects into a DataFrame.
    validator : Any
        Validator component that checks DataFrame quality.
    metadata : Any
        Metadata component that enriches the DataFrame with Bronze metadata.
    writer : Any
        Writer component that persists the DataFrame to Bronze storage.
    logger : logging.Logger | None, optional
        Optional logger for pipeline diagnostics.
    """

    def __init__(
        self,
        reader: Any,
        validator: Any,
        metadata: Any,
        writer: Any,
        logger: logging.Logger | None = None,
    ) -> None:
        """
        Initialize the pipeline with injected dependencies.

        Parameters
        ----------
        reader : Any
            Reader dependency.
        validator : Any
            Validator dependency.
        metadata : Any
            Metadata dependency.
        writer : Any
            Writer dependency.
        logger : logging.Logger | None, optional
            Logger dependency.
        """
        self.reader = reader
        self.validator = validator
        self.metadata = metadata
        self.writer = writer
        self.logger = logger or logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def run(self, data: Any, output_path: str, **writer_options: Any) -> Any:
        """
        Execute the Bronze orchestration flow.

        The pipeline executes the injected components in the following ordering:

        1. ``reader.read(data)``
        2. ``validator.validate(dataframe)``
        3. ``metadata.enrich(dataframe)``
        4. ``writer.write(dataframe, output_path, **writer_options)``

        Parameters
        ----------
        data : Any
            Input payload consumed by the reader.
        output_path : str
            Destination path passed to the writer.
        **writer_options : Any
            Optional writer configuration forwarded directly to the writer.

        Returns
        -------
        Any
            The exact value returned by the injected writer.

        Raises
        ------
        BronzePipelineException
            If any orchestration step fails.
        """
        self.logger.info("pipeline_started", extra={"output_path": output_path})

        try:
            dataframe = self.reader.read(data)
            self.logger.info("reader_completed")

            dataframe = self.validator.validate(dataframe)
            self.logger.info("validator_completed")

            dataframe = self.metadata.enrich(dataframe)
            self.logger.info("metadata_completed")

            result = self.writer.write(dataframe, output_path, **writer_options)
            self.logger.info("writer_completed", extra={"output_path": output_path})
        except Exception as exc:
            self.logger.warning(
                "pipeline_failed",
                extra={"output_path": output_path, "error": str(exc)},
            )
            raise BronzePipelineException("Bronze pipeline execution failed.") from exc

        self.logger.info("pipeline_finished", extra={"output_path": output_path})
        return result
