"""
StockFlow application composition root.

This module is the top-level entry point for the Bronze-only deployment
currently supported by the project. It is responsible for composing the
application from already-specified components and running the Bronze job.
It does not perform business logic, data transformation, validation,
API calls, or persistence directly.
"""

from __future__ import annotations

import logging
from typing import Any

from include.api.adapter import ProviderAdapter
from include.api.normalizer import StockNormalizer
from include.api.providers.finnhub import FinnhubProvider
from include.core.exceptions import BronzeJobException
from include.framework.bronze.metadata import BronzeMetadata
from include.framework.bronze.pipeline import BronzePipeline
from include.framework.bronze.reader import BronzeReader
from include.framework.bronze.validator import BronzeValidator
from include.framework.bronze.writer import BronzeWriter
from include.jobs.bronze.bronze_job import BronzeJob


class StockFlowAPIClient:
    """
    Thin API orchestration client for Bronze job composition.

    This object is the application-level bridge between the provider layer and
    the Bronze pipeline. It stores the adapter and normalizer dependencies and
    exposes a single ``fetch(symbol)`` method that returns the normalized quote
    payload expected by the Bronze reader.
    """

    def __init__(self, adapter: ProviderAdapter, normalizer: StockNormalizer) -> None:
        """
        Initialize the API client composition.

        Parameters
        ----------
        adapter : ProviderAdapter
            Provider execution orchestration dependency.
        normalizer : StockNormalizer
            Normalization dependency.
        """
        self._adapter = adapter
        self._normalizer = normalizer

    def fetch(self, symbol: str) -> dict[str, Any]:
        """
        Fetch and normalize a stock quote for the supplied symbol.

        Parameters
        ----------
        symbol : str
            Stock ticker symbol.

        Returns
        -------
        dict[str, Any]
            Unified normalized quote payload ready for Bronze input.
        """
        raw_payload = self._adapter.fetch(symbol)
        provider_name = self._adapter._provider.provider_name
        return self._normalizer.normalize(
            provider_name=provider_name,
            raw_json=raw_payload,
            symbol=symbol,
        )


from typing import Any

from pyspark.sql import SparkSession


def _build_spark_session() -> Any:
    """
    Build and return the SparkSession used by the Bronze layer.

    Returns
    -------
    SparkSession
    """

    return (
        SparkSession.builder
        .appName("stockflow")

        # ------------------------------------------------------------
        # Connect Spark Cluster
        # ------------------------------------------------------------
        .master("spark://stockflow-spark-master:7077")

        # ------------------------------------------------------------
        # MinIO (S3A)
        # ------------------------------------------------------------
        .config("spark.hadoop.fs.s3a.endpoint", "http://stockflow-minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )

        # ------------------------------------------------------------
        # Spark Deploy
        # ------------------------------------------------------------
        .config("spark.submit.deployMode", "client")

        # ------------------------------------------------------------
        # Performance
        # ------------------------------------------------------------
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")

        .getOrCreate()
    )


def main(output_path: str = "/tmp/stockflow") -> int:
    """
    Compose and run the Bronze application stack.

    Parameters
    ----------
    output_path : str, optional
        Output path forwarded to the Bronze writer.

    Returns
    -------
    int
        Exit code. ``0`` indicates successful completion.

    Raises
    ------
    BronzeJobException
        Propagated when the composition or Bronze job fails unexpectedly.
    """
    logger = logging.getLogger("stockflow")
    logger.info("application_started")

    try:
        spark = _build_spark_session()
        logger.info(f"Spark master = {spark.sparkContext.master}")
        logger.info(f"Spark version = {spark.version}")
        logger.info(spark.sparkContext._jsc.sc().listJars())
        logger.info("dependencies_initialized")

        provider = FinnhubProvider(conn_id="finnhub")
        adapter = ProviderAdapter(provider=provider, max_retry=3)
        normalizer = StockNormalizer()
        api_client = StockFlowAPIClient(adapter=adapter, normalizer=normalizer)

        reader = BronzeReader(spark=spark)
        validator = BronzeValidator()
        metadata = BronzeMetadata()
        writer = BronzeWriter()
        pipeline = BronzePipeline(reader=reader, validator=validator, metadata=metadata, writer=writer)
        job = BronzeJob(api_client=api_client, pipeline=pipeline)

        logger.info("bronze_job_started")
        job.run()
        logger.info("bronze_job_finished")
        logger.info("application_finished")
        return 0
    except Exception as exc:
        logger.exception("application_failed")
        raise BronzeJobException("Application startup failed.") from exc


if __name__ == "__main__":
    raise SystemExit(main())
