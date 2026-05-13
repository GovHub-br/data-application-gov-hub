import logging
import os
import random
import string
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


def generate_test_dataframe(n: int) -> pl.DataFrame:
    """Generate a random DataFrame that mimics a real government entity extract."""
    return pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "name": [
                "".join(random.choices(string.ascii_uppercase, k=6)) for _ in range(n)
            ],
            "value": [round(random.uniform(0.0, 100_000.0), 2) for _ in range(n)],
            "category": [random.choice(["A", "B", "C", "D"]) for _ in range(n)],
            "active": [random.choice([True, False]) for _ in range(n)],
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
        n_records = random.randint(50, 200)

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


dag_instance = test_source_ingest_dag()
