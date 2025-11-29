import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from time_utils import brasilia_now_iso
from postgres_helpers import get_postgres_conn
from cliente_ted import ClienteTed
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("programa_beneficiario_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ted_api", "programa_beneficiario"],
)
def api_programa_beneficiario_dag() -> None:
    @task
    def fetch_and_store_programa_beneficiario() -> None:
        logging.info("Starting api_programa_beneficiario_dag DAG")
        api = ClienteTed()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        tx_codigo_siorgs = "7"

        beneficiario = api.get_ted_by_programa_beneficiario(tx_codigo_siorgs)
        if beneficiario:
            logging.info(
                f"Tipo de beneficiario: {type(beneficiario)}, Conteúdo: {beneficiario}"
            )
            logging.info("Inserting beneficiario into PostgreSQL")
            unique_id_programas = [
                {"id_programa": id_prog}
                for id_prog in {b["id_programa"] for b in beneficiario}
            ]

            for programa in unique_id_programas:
                programa["dt_ingest"] = brasilia_now_iso()

            db.insert_data(
                unique_id_programas,
                "programas",
                primary_key=["id_programa"],
                conflict_fields=["id_programa"],
                schema="transfere_gov",
            )
        else:
            logging.warning(
                f"No beneficiario found for tx_codigo_siorg: {tx_codigo_siorgs}"
            )

    fetch_and_store_programa_beneficiario()


dag_instace = api_programa_beneficiario_dag()
