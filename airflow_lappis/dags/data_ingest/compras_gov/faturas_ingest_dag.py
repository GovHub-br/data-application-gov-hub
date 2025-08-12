import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from cliente_contratos import ClienteContratos
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["faturas_api", "compras_gov"],
)
def api_faturas_dag() -> None:
    """DAG para buscar e armazenar faturas de uma API no PostgreSQL."""

    @task
    def fetch_faturas() -> None:
        logging.info("[faturas_ingest_dag.py] Starting fetch_faturas task")
        api = ClienteContratos()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        contratos_ids = db.get_contratos_ids()

        for contrato_id in contratos_ids:
            try:
                logging.info(
                    f"[faturas_ingest_dag.py] Fetching faturas for contrato ID: "
                    f"{contrato_id}"
                )
                faturas = api.get_faturas_by_contrato_id(str(contrato_id))

                # Adicionar dt_ingest a cada fatura
                if faturas:
                    for fatura in faturas:
                        fatura["dt_ingest"] = datetime.now().isoformat()

                logging.info(
                    f"[faturas_ingest_dag.py] Inserting faturas for contrato ID: "
                    f"{contrato_id} into PostgreSQL"
                )
                db.insert_data(
                    faturas,
                    "faturas",
                    conflict_fields=["id"],
                    primary_key=["id"],
                    schema="compras_gov",
                )
            except Exception as e:
                logging.error(
                    f"[faturas_ingest_dag.py] Error fetching faturas for contrato ID "
                    f"{contrato_id}: {e}"
                )

    fetch_faturas()


dag_instance = api_faturas_dag()
