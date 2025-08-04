import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_ted import ClienteTed
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
    tags=["ted_api", "planos_acao"],
)
def api_planos_acao_dag() -> None:

    @task
    def fetch_and_store_planos_acao() -> None:
        logging.info("Starting api_planos_acao_dag DAG")
        api = ClienteTed()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        id_programas = db.get_id_programas()

        total_processed = 0
        for id_programa in id_programas:
            planos_acao_data = api.get_planos_acao_by_id_programa(id_programa)
            if planos_acao_data:

                db.insert_data(
                    planos_acao_data,
                    "planos_acao",
                    primary_key=["id_plano_acao"],
                    conflict_fields=["id_plano_acao"],
                    schema="transfere_gov",
                )

                total_processed += 1
                if total_processed % 10 == 0:
                    logging.info(f"Processed {total_processed} planos de ação")
            else:
                logging.warning(f"No planos de ação found for id_programa: {id_programa}")

        logging.info(f"Completed processing {total_processed} planos de ação")

    fetch_and_store_planos_acao()


dag_instance = api_planos_acao_dag()
