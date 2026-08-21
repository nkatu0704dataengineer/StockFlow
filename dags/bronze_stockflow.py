"""Thin Airflow DAG entry point for the Bronze StockFlow application.

This DAG intentionally does not contain business logic, provider logic,
normalization logic, Spark logic, persistence logic, or any custom retry
handling. It only schedules a single task that invokes the application
composition root via ``main()``.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from main import main


DEFAULT_ARGS = {
    "owner": "stockflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
}


@dag(
    dag_id="bronze_stockflow",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=None,
    catchup=False,
    tags=["Stockflow","API","Adapter", "Bronze"],
)
def bronze_stockflow() -> None:
    """Create the single Bronze scheduling task.

    Notes
    -----
    The DAG is intentionally thin. It does not coordinate providers,
    validators, readers, metadata, writers, or Spark directly.
    """

    @task(task_id="run_bronze_pipeline")
    def run_bronze_pipeline() -> int:
        """Invoke the application entry point.

        Returns
        -------
        int
            Exit code returned by the composition root.
        """
        return main()

    run_bronze_pipeline()


bronze_stockflow = bronze_stockflow()
