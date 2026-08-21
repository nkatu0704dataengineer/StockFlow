"""
Bronze writer.

Persist validated Spark DataFrames to the Bronze layer.

Responsibilities
----------------
- Validate write configuration.
- Persist Spark DataFrame.

The writer does NOT:
- Call APIs.
- Validate business data.
- Normalize schemas.
- Create SparkSession.
- Build storage paths.
"""

from __future__ import annotations

import logging
from typing import Any

from include.core.exceptions import BronzeWriterException


class BronzeWriter:
    """
    Persist validated Spark DataFrames to Bronze storage.
    """

    SUPPORTED_FORMATS = {"parquet", "delta"}
    SUPPORTED_MODES = {"append", "overwrite"}

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write(
        self,
        dataframe: Any,
        output_path: str,
        format: str = "parquet",
        mode: str = "overwrite",
        partition_by: list[str] | None = None,
    ) -> str:
        """
        Persist a Spark DataFrame to the provided Bronze output path.

        Parameters
        ----------
        dataframe
            Spark DataFrame.

        output_path
            Fully qualified storage path to persist the DataFrame to.

        format
            parquet | delta

        mode
            append | overwrite

        partition_by
            Optional Spark partition columns.

        Returns
        -------
        str
            Output path.
        """

        self._validate_format(format)
        self._validate_mode(mode)

        self.logger.info(
            "writer_started",
            extra={
                "output_path": output_path,
                "format": format,
                "mode": mode,
            },
        )

        try:
            self._write(
                dataframe=dataframe,
                output_path=output_path,
                format=format,
                mode=mode,
                partition_by=partition_by,
            )
        except Exception as exc:
            self.logger.exception(
                "writer_failed",
                extra={
                    "output_path": output_path,
                    "format": format,
                    "mode": mode,
                },
            )
            raise BronzeWriterException("Spark write failed.") from exc

        self.logger.info(
            "writer_finished",
            extra={
                "output_path": output_path,
                "format": format,
                "mode": mode,
            },
        )

        return output_path

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_format(self, format: str) -> None:
        if format not in self.SUPPORTED_FORMATS:
            raise BronzeWriterException(
                f"Unsupported format '{format}'. "
                f"Supported formats: {', '.join(sorted(self.SUPPORTED_FORMATS))}."
            )

    def _validate_mode(self, mode: str) -> None:
        if mode not in self.SUPPORTED_MODES:
            raise BronzeWriterException(
                f"Unsupported mode '{mode}'. "
                f"Supported modes: {', '.join(sorted(self.SUPPORTED_MODES))}."
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _write(
        self,
        dataframe: Any,
        output_path: str,
        format: str,
        mode: str,
        partition_by: list[str] | None,
    ) -> None:
        """
        Execute Spark DataFrame write.
        """

        writer = dataframe.write.mode(mode).format(format)

        if partition_by:
            writer = writer.partitionBy(*partition_by)
        self.logger.info(f"Saving dataframe to: {output_path}")
        writer.save(output_path)
        self.logger.info("Spark save completed successfully.")