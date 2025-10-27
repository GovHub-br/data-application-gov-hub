import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_ted import ClienteTed


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["notas de credito", "ted_api"],
)
def notas_de_credito_dag() -> None:
    @task
    def fetch_and_store_notas_de_credito() -> None:
        logging.info("Iniciando fetch_and_store_notas_de_credito")

        api = ClienteTed()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        id_planos_acao = db.get_id_planos_acao()

        for id_plano_acao in id_planos_acao:
            notas_de_credito = api.get_notas_de_credito_by_id_plano_acao(id_plano_acao)
            if notas_de_credito:
                # Adicionar dt_ingest a cada nota
                for nota in notas_de_credito:
                    nota["dt_ingest"] = datetime.now().isoformat()

                db.insert_data(
                    notas_de_credito,
                    "notas_de_credito",
                    conflict_fields=["id_nota"],
                    primary_key=["id_nota"],
                    schema="transfere_gov",
                )
            else:
                logging.warning(
                    f"Nenhuma nota de crédito encontrada plano de ação {id_plano_acao}"
                )

    fetch_and_store_notas_de_credito()


dag_instance = notas_de_credito_dag()
