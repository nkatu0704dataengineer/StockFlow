"""
Bronze reader bridge.

This module implements the BronzeReader class, which is responsible only for
converting already-normalized Python objects into a Spark DataFrame. The
reader does not validate records, add metadata, perform business
transformation, or interact with external systems.
"""

from __future__ import annotations

import logging
from typing import Any

from include.core.exceptions import BronzeReaderException
from include.core.schemas import StockQuoteSchema


class BronzeReader:
    """
    Convert normalized quote objects into Spark DataFrames.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session used to materialize a DataFrame.
    """

    def __init__(self, spark: Any) -> None:
        """
        Initialize the reader with a Spark session.

        Parameters
        ----------
        spark : SparkSession
            Spark session instance.
        """
        self._spark = spark
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def read(self, data: StockQuoteSchema | dict[str, Any] | list[dict[str, Any]]) -> Any:
        """
        Convert supported in-memory quote payloads into a Spark DataFrame.

        Parameters
        ----------
        data : StockQuoteSchema | dict[str, Any] | list[dict[str, Any]]
            Input payload already normalized by the API layer.

        Returns
        -------
        Any
            Spark DataFrame produced from the input payload.

        Raises
        ------
        BronzeReaderException
            If the input type is unsupported, the list is empty, or Spark
            DataFrame creation fails.
        """
        input_type = type(data).__name__
        self.logger.info("reader_started", extra={"input_type": input_type})

        try:
            if isinstance(data, StockQuoteSchema):
                result = self._read_schema_object(data)
            elif isinstance(data, dict):
                result = self._read_dict(data)
            elif isinstance(data, list):
                result = self._read_list(data)
            else:
                self.logger.warning(
                    "unsupported_input",
                    extra={"input_type": input_type},
                )
                raise BronzeReaderException(
                    f"Unsupported input type for BronzeReader.read(): {input_type}."
                )
        except BronzeReaderException:
            raise
        except Exception as exc:
            raise BronzeReaderException("Spark DataFrame creation failed.") from exc

        self.logger.info(
            "reader_finished",
            extra={"input_type": input_type, "records_loaded": self._count_records(data)},
        )
        return result

    def _read_schema_object(self, data: StockQuoteSchema) -> Any:
        """
        Convert a StockQuoteSchema object into a Spark DataFrame.

        Parameters
        ----------
        data : StockQuoteSchema
            Canonical schema object already normalized for downstream usage.

        Returns
        -------
        Any
            Spark DataFrame containing one record.
        """
        self.logger.debug("schema_object_to_dataframe")
        return self._read_dict(data.to_dict())

    def _read_dict(self, data: dict[str, Any]) -> Any:
        """
        Convert a single dictionary payload into a Spark DataFrame.

        Parameters
        ----------
        data : dict[str, Any]
            One normalized quote row.

        Returns
        -------
        Any
            Spark DataFrame containing one row.
        """
        self.logger.debug("dict_to_dataframe")
        try:
            self.logger.info(f"Reader payload: {data}")
            self.logger.info("Creating Spark DataFrame...")
            dataframe = self._spark.createDataFrame([data])
            self.logger.info("Spark DataFrame created")
            dataframe.printSchema()
            self.logger.info("Schema printed")
            self.logger.info("records_loaded", extra={"records_loaded": 1})
            return dataframe
        except Exception as exc:
            self.logger.exception(
                "create_dataframe_failed",
                extra={
                    "payload": str(data),
                    "payload_type": type(data).__name__,
               },
            )
            raise BronzeReaderException(str(exc)) from exc

    def _read_list(self, data: list[dict[str, Any]]) -> Any:
        """
        Convert a list of dictionaries into a Spark DataFrame.

        Parameters
        ----------
        data : list[dict[str, Any]]
            Normalized quote rows.

        Returns
        -------
        Any
            Spark DataFrame containing all rows in the input list.

        Raises
        ------
        BronzeReaderException
            If the provided list is empty.
        """
        if not data:
            self.logger.warning("empty_dataset")
            raise BronzeReaderException("Empty dataset.")

        self.logger.debug("list_to_dataframe")
        try:
            dataframe = self._spark.createDataFrame(data)
            self.logger.info("records_loaded", extra={"records_loaded": len(data)})
            return dataframe
        except Exception as exc:
            self.logger.exception(
                "create_dataframe_failed",
                extra={
                   "records": len(data),
                   "first_record": str(data[0]) if data else None,
                },
            )
            raise BronzeReaderException(str(exc)) from exc

    def _count_records(self, data: StockQuoteSchema | dict[str, Any] | list[dict[str, Any]]) -> int:
        """
        Count the logical number of row records represented by the input.

        Parameters
        ----------
        data : StockQuoteSchema | dict[str, Any] | list[dict[str, Any]]
            Reader input payload.

        Returns
        -------
        int
            Number of records represented by the input object.
        """
        if isinstance(data, StockQuoteSchema):
            return 1
        if isinstance(data, dict):
            return 1
        if isinstance(data, list):
            return len(data)
        return 0
