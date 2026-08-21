"""
Bronze metadata enrichment.

This module implements BronzeMetadata, which is responsible only for adding
Bronze-specific metadata columns to a validated Spark DataFrame. It does not
validate rows, rewrite source quote fields, or route persistence.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any
from pyspark.sql.functions import lit

from include.core.exceptions import BronzeMetadataException


class BronzeMetadata:
    """
    Enrich a validated Spark DataFrame with Bronze metadata columns.

    The metadata layer only appends Bronze operational metadata and preserves
    the source quote identity fields already supplied by the API layer.
    """

    PROTECTED_COLUMNS = {
        "symbol",
        "timestamp",
        "ingested_at",
        "normalizer_version",
        "source_provider",
    }

    def __init__(self) -> None:
        """
        Initialize the metadata logger.
        """
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def enrich(self, dataframe: Any) -> Any:
        """
        Enrich a Spark DataFrame with Bronze metadata columns.

        Parameters
        ----------
        dataframe : Any
            Spark DataFrame that has already passed Bronze validation.

        Returns
        -------
        Any
            The same DataFrame object after Bronze metadata columns have been
            appended.

        Raises
        ------
        BronzeMetadataException
            If the input is not a Spark DataFrame-like object or enrichment
            fails at runtime.
        """
        if not self._is_dataframe_like(dataframe):
            self.logger.warning("metadata_failed", extra={"reason": "input_not_dataframe"})
            raise BronzeMetadataException("Input is not a Spark DataFrame.")

        self.logger.info("metadata_started")

        try:
            bronze_ingested_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            bronze_batch_id = str(uuid.uuid4())
            run_id = str(uuid.uuid4())

            dataframe = dataframe.withColumn("bronze_ingested_at", lit(bronze_ingested_at))
            dataframe = dataframe.withColumn("bronze_batch_id", lit(bronze_batch_id))
            dataframe = dataframe.withColumn("bronze_job_name", lit("bronze_pipeline"))
            dataframe = dataframe.withColumn("bronze_layer", lit("bronze"))
            dataframe = dataframe.withColumn("bronze_version", lit("1.0"))
            dataframe = dataframe.withColumn("run_id", lit(run_id))
        except Exception as exc:
            self.logger.warning("metadata_failed", extra={"reason": "metadata_enrichment_failed"})
            raise BronzeMetadataException(f"Metadata enrichment failed : {exc}") from exc

        self.logger.info(
            "metadata_columns_added",
            extra={
                "columns_added": [
                    "bronze_ingested_at",
                    "bronze_batch_id",
                    "bronze_job_name",
                    "bronze_layer",
                    "bronze_version",
                    "run_id",
                ]
            },
        )
        self.logger.info("metadata_finished")
        return dataframe

    def _is_dataframe_like(self, dataframe: Any) -> bool:
        """
        Return whether the provided object exposes the minimal Spark DataFrame
        writer surface expected by Bronze metadata enrichment.

        Parameters
        ----------
        dataframe : Any
            Candidate object to inspect.

        Returns
        -------
        bool
            ``True`` when the object appears to be DataFrame-like.
        """
        return hasattr(dataframe, "withColumn") and hasattr(dataframe, "columns")
