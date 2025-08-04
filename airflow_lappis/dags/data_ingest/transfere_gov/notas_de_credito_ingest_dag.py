import logging
import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
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

        orgao_alvo = Variable.get("airflow_orgao", default_var=None)
        if not orgao_alvo:
            logging.error("Variável airflow_orgao não definida!")
            raise ValueError("airflow_orgao não definida")

        orgaos_config_str = Variable.get("airflow_variables", default_var="{}")
        orgaos_config = yaml.safe_load(orgaos_config_str)

        ug_codes = orgaos_config.get(orgao_alvo, {}).get("codigos_ug", [])

        if not ug_codes:
            logging.warning(f"Nenhum código UG encontrado para o órgão '{orgao_alvo}'")
            return

        api = ClienteTed()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        for ug_code in ug_codes:
            notas_de_credito = api.get_notas_de_credito_by_ug(ug_code)
            if notas_de_credito:
                db.insert_data(
                    notas_de_credito,
                    "notas_de_credito",
                    conflict_fields=["id_nota"],
                    primary_key=["id_nota"],
                    schema="transfere_gov",
                )
            else:
                logging.warning(f"Nenhuma nota de crédito encontrada para UG {ug_code}")

    fetch_and_store_notas_de_credito()


dag_instance = notas_de_credito_dag()
