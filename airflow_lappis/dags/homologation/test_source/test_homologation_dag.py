import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

from homologation_flow import load_landing_files, validate_dataframe, write_to_postgres
from postgres_helpers import get_postgres_conn

SOURCE = "test_source"
ENTITY = "entities"
EXPECTED_COLUMNS = ["id", "name", "value", "category", "active", "dt_ingest"]
NOT_NULL_COLUMNS = ["id", "dt_ingest"]
KEY_COLUMNS = ["id"]
MIN_ROWS = 1


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "govhub",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["test", "landing_zone", "homologation"],
)
def test_source_homologation_dag() -> None:

    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="test_source_ingest_dag",
        external_task_id=None,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task
    def quality_checks(**context: Any) -> dict:
        """Read landing zone files for the run date and assert data quality."""
        run_date = context["data_interval_start"].date()
        files, df = load_landing_files(SOURCE, ENTITY, run_date)

        all_passed, results = validate_dataframe(
            df, EXPECTED_COLUMNS, NOT_NULL_COLUMNS, KEY_COLUMNS, MIN_ROWS
        )
        for r in results:
            logging.log(
                logging.INFO if r.passed else logging.WARNING,
                f"[{SOURCE}] {r.check_name}: {r.message}",
            )

        if not all_passed:
            failed = [r.check_name for r in results if not r.passed]
            raise ValueError(f"[{SOURCE}] Quality checks failed: {failed}")

        return {"files": files, "rows": len(df)}

    @task
    def store_in_warehouse(check_result: dict) -> None:
        """Write quality-checked data to the landing schema in Postgres."""
        files: list[str] = check_result["files"]
        # Re-read so the task is independently restartable
        from landing_zone import read_parquet
        import polars as pl

        df = pl.concat([read_parquet(f) for f in files])
        rows = write_to_postgres(
            df,
            table_name=f"{SOURCE}_{ENTITY}",
            schema="landing",
            conn_str=get_postgres_conn(),
        )
        logging.info(f"[{SOURCE}] Stored {rows} rows in landing.{SOURCE}_{ENTITY}")

    check_result = quality_checks()
    wait_for_ingest >> check_result
    store_in_warehouse(check_result)  # type: ignore


test_source_homologation_dag()
