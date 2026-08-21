"""
Bronze validator.

This module implements BronzeValidator, a single-purpose Spark DataFrame
quality gate for Bronze ingestion. It is intentionally limited to checking
required schema columns, datatype compatibility, and duplicate keys based on
``(symbol, timestamp)``.
"""

from __future__ import annotations

import logging
from typing import Any

from include.core.exceptions import BronzeValidatorException


class BronzeValidator:
    """
    Validate a Spark DataFrame before it is persisted to Bronze storage.

    Notes
    -----
    The validator only inspects the DataFrame shape and quality. It does not
    modify columns, cast values, enrich rows, or perform business analysis.
    """

    REQUIRED_COLUMNS = (
        "symbol",
        "source_provider",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "currency",
        "exchange",
        "ingested_at",
        "normalizer_version",
    )

    def __init__(self) -> None:
        """
        Initialize the validator logger.
        """
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def validate(self, df: Any) -> Any:
        """
        Validate a Spark DataFrame using the Bronze quality gate.

        Parameters
        ----------
        df : Any
            Spark DataFrame to inspect.

        Returns
        -------
        Any
            The original DataFrame instance when validation succeeds.

        Raises
        ------
        BronzeValidatorException
            If any Bronze quality rule fails.
        """
        self.logger.info("validation_started")

        self._validate_required_columns(df)
        self._validate_datatypes(df)
        self._validate_duplicates(df)

        self.logger.info("validation_finished")
        return df

    def _validate_required_columns(self, df: Any) -> None:
        """
        Check that the DataFrame contains all required unified quote columns.

        Parameters
        ----------
        df : Any
            Spark DataFrame to inspect.

        Raises
        ------
        BronzeValidatorException
            If any required column is missing.
        """
        columns = list(getattr(df, "columns", []))
        missing = [column for column in self.REQUIRED_COLUMNS if column not in columns]

        if missing:
            self.logger.warning(
                "validation_failed",
                extra={"reason": "missing_required_columns", "missing_columns": missing},
            )
            raise BronzeValidatorException(
                f"Missing required columns: {', '.join(missing)}"
            )

        self.logger.info(
            "required_columns_validated",
            extra={"required_columns": list(self.REQUIRED_COLUMNS)},
        )

    def _validate_datatypes(self, df: Any) -> None:
        """
        Check the DataFrame column datatypes against Bronze schema expectations.

        Parameters
        ----------
        df : Any
            Spark DataFrame to inspect.

        Raises
        ------
        BronzeValidatorException
            If any field has an invalid datatype.
        """
        dtype_rows = getattr(df, "dtypes", [])
        dtype_map = {
            str(name).lower(): str(dtype).lower()
            for name, dtype in dtype_rows
        }

        expected_rules = {
            "symbol": "string",
            "source_provider": "string",
            "timestamp": "bigint",
            "open": "numeric",
            "high": "numeric",
            "low": "numeric",
            "close": "numeric",
            "volume": "numeric",
            "currency": "string",
            "exchange": "string",
            "ingested_at": "string",
            "normalizer_version": "string",
        }

        for field_name, expected in expected_rules.items():
            actual = dtype_map.get(field_name)
            if actual is None:
                continue

            if expected == "string":
                if not self._is_string_dtype(actual):
                    self.logger.warning(
                        "validation_failed",
                        extra={"reason": "invalid_datatype", "column": field_name, "actual": actual},
                    )
                    raise BronzeValidatorException(
                        f"Invalid datatype for column '{field_name}': expected string, got {actual}."
                    )
            elif expected == "bigint":
                if not self._is_bigint_dtype(actual):
                    self.logger.warning(
                        "validation_failed",
                        extra={"reason": "invalid_datatype", "column": field_name, "actual": actual},
                    )
                    raise BronzeValidatorException(
                        f"Invalid datatype for column '{field_name}': expected bigint/long, got {actual}."
                    )
            elif expected == "numeric":
                if not self._is_numeric_dtype(actual):
                    self.logger.warning(
                        "validation_failed",
                        extra={"reason": "invalid_datatype", "column": field_name, "actual": actual},
                    )
                    raise BronzeValidatorException(
                        f"Invalid datatype for column '{field_name}': expected numeric, got {actual}."
                    )

        self.logger.info("datatype_validated")

    def _validate_duplicates(self, df: Any) -> None:
        """
        Detect duplicate rows using the Bronze canonical key ``(symbol, timestamp)``.

        Parameters
        ----------
        df : Any
            Spark DataFrame to inspect.

        Raises
        ------
        BronzeValidatorException
            If a duplicate ``(symbol, timestamp)`` pair is found.
        """
        grouped = df.groupBy("symbol", "timestamp")

        try:
            grouped_counts = grouped.count()
            if hasattr(grouped_counts, "collect"):
                duplicate_rows = grouped_counts.collect()
            else:
                duplicate_rows = grouped.collect()
        except Exception:
            duplicate_rows = grouped.collect()

        duplicates_found = False
        if hasattr(duplicate_rows, "__iter__") and not isinstance(duplicate_rows, (str, bytes)):
            for row in duplicate_rows:
                try:
                    count_value = row["count"] if isinstance(row, dict) else row.count
                except Exception:
                    count_value = None

                if isinstance(count_value, int) and count_value > 1:
                    duplicates_found = True
                    break

                if isinstance(row, dict) and row.get("count", 0) > 1:
                    duplicates_found = True
                    break

        if duplicates_found:
            self.logger.warning("validation_failed", extra={"reason": "duplicate_records_detected"})
            raise BronzeValidatorException("Duplicate records detected for key (symbol, timestamp).")

        self.logger.info("duplicate_validated")

    def _is_string_dtype(self, dtype: str) -> bool:
        """
        Return whether the supplied Spark dtype is a string-like dtype.

        Parameters
        ----------
        dtype : str
            Spark dtype representation.

        Returns
        -------
        bool
            ``True`` when the dtype is string-like.
        """
        return dtype in {"string", "varchar", "char", "text"}

    def _is_bigint_dtype(self, dtype: str) -> bool:
        """
        Return whether the supplied Spark dtype is a bigint/long-like dtype.

        Parameters
        ----------
        dtype : str
            Spark dtype representation.

        Returns
        -------
        bool
            ``True`` when the dtype is a valid Bronze timestamp dtype.
        """
        return dtype in {"bigint", "long", "int", "integer"}

    def _is_numeric_dtype(self, dtype: str) -> bool:
        """
        Return whether the supplied Spark dtype is numeric.

        Parameters
        ----------
        dtype : str
            Spark dtype representation.

        Returns
        -------
        bool
            ``True`` when the dtype is numeric.
        """
        numeric_types = {
            "double",
            "float",
            "decimal",
            "bigint",
            "long",
            "int",
            "integer",
            "smallint",
            "tinyint",
        }
        return dtype in numeric_types
