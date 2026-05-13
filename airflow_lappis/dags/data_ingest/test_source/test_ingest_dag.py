import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

import polars as pl
from airflow.decorators import dag, task

from landing_zone import build_landing_path, write_parquet
from postgres_helpers import get_postgres_conn

# Keep Postgres writes available behind a flag so the pattern is ready for
# real DAGs that still need a direct DB copy during the transition period.
ENABLE_POSTGRES_INGEST = os.getenv("ENABLE_POSTGRES_INGEST", "false").lower() == "true"

SOURCE = "test_source"
ENTITY = "entities"


_CATEGORIES = ["A", "B", "C", "D"]


def generate_test_dataframe(n: int) -> pl.DataFrame:
    """Generate a deterministic DataFrame that mimics a real government entity extract."""
    return pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "name": [f"NAME_{i:04d}" for i in range(1, n + 1)],
            "value": [float(i * 100) for i in range(1, n + 1)],
            "category": [_CATEGORIES[i % len(_CATEGORIES)] for i in range(n)],
            "active": [i % 2 == 0 for i in range(n)],
            "dt_ingest": [datetime.now().isoformat()] * n,
        }
    )


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "govhub",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["test", "landing_zone", "ingestion"],
)
def test_source_ingest_dag() -> None:

    @task
    def extract_and_store(**context: Any) -> str:
        """Generate random records and write them to the landing zone as Parquet."""
        run_date = context["data_interval_start"].date()
        run_id = str(uuid.uuid4())[:8]
        n_records = 100

        logging.info(f"[{SOURCE}] Generating {n_records} records for {run_date}")
        df = generate_test_dataframe(n_records)

        path = build_landing_path(SOURCE, ENTITY, run_date, run_id)
        write_parquet(df, path)

        if ENABLE_POSTGRES_INGEST:
            from cliente_postgres import ClientPostgresDB

            db = ClientPostgresDB(get_postgres_conn())
            db.insert_data(
                df.to_pandas().to_dict(orient="records"),
                table_name=ENTITY,
                schema=SOURCE,
            )
            logging.info(
                f"[{SOURCE}] Also written to Postgres (ENABLE_POSTGRES_INGEST=true)"
            )

        return path

    extract_and_store()


test_source_ingest_dag()
