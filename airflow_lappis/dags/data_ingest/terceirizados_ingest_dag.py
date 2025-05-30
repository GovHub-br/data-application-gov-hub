import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_contratos import ClienteContratos
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["terceirizados_api", "compras_gov"],
)
def api_terceirizados_dag() -> None:
    """DAG para buscar e armazenar dados de terceirizados de uma API."""

    @task
    def fetch_terceirizados() -> None:
        logging.info("[terceirizados_ingest_dag.py] Starting fetch_terceirizados task")
        api = ClienteContratos()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        contratos_ids = db.get_contratos_ids()

        for contrato_id in contratos_ids:
            try:
                logging.info(f"Fetching terceirizados for contrato ID: " f"{contrato_id}")
                terceirizados = api.get_terceirizados_by_contrato_id(str(contrato_id))

                logging.info(
                    f"Inserting terceirizados for contrato ID: "
                    f"{contrato_id} into PostgreSQL"
                )
                db.insert_data(
                    terceirizados,
                    "terceirizados",
                    conflict_fields=["id"],
                    primary_key=["id"],
                    schema="compras_gov",
                )
            except Exception as e:
                logging.error(
                    f"[terceirizados_ingest_dag.py] Error fetching terceirizados for "
                    f"contrato ID {contrato_id}: {e}"
                )

    fetch_terceirizados()


dag_instance = api_terceirizados_dag()
