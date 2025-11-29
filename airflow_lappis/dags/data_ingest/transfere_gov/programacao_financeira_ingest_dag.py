import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from time_utils import brasilia_now_iso
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_ted import ClienteTed


@dag(
    schedule_interval=get_dynamic_schedule("programacao_financeira_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["programacao_financeira", "ted_api"],
)
def programacao_financeira_dag() -> None:
    @task
    def fetch_and_store_programacao_financeira() -> None:
        logging.info("Iniciando fetch_and_store_programacao_financeira")

        api = ClienteTed()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        id_planos_acao = db.get_id_planos_acao()

        for id_plano_acao in id_planos_acao:
            programacao_financeira = api.get_programacao_financeira_by_id_plano_acao(
                id_plano_acao
            )
            if programacao_financeira:
                # Adicionar dt_ingest a cada item
                for item in programacao_financeira:
                    item["dt_ingest"] = brasilia_now_iso()

                db.insert_data(
                    programacao_financeira,
                    "programacao_financeira",
                    conflict_fields=["id_programacao"],
                    primary_key=["id_programacao"],
                    schema="transfere_gov",
                )
            else:
                logging.warning(
                    f"Nenhuma programação financeira encontrada "
                    f"plano de ação {id_plano_acao}"
                )

    fetch_and_store_programacao_financeira()


dag_instance = programacao_financeira_dag()
