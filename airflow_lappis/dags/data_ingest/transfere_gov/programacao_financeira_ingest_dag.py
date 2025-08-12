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
    tags=["programacao_financeira", "ted_api"],
)
def programacao_financeira_dag() -> None:
    @task
    def fetch_and_store_programacao_financeira() -> None:
        logging.info("Iniciando fetch_and_store_programacao_financeira")

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
            programacao_financeira = api.get_programacao_financeira_by_ug(ug_code)
            if programacao_financeira:
                # Adicionar dt_ingest a cada item
                for item in programacao_financeira:
                    item["dt_ingest"] = datetime.now().isoformat()

                db.insert_data(
                    programacao_financeira,
                    "programacao_financeira",
                    conflict_fields=["id_programacao"],
                    primary_key=["id_programacao"],
                    schema="transfere_gov",
                )
            else:
                logging.warning(
                    f"Nenhuma programação financeira encontrada para UG {ug_code}"
                )

    fetch_and_store_programacao_financeira()


dag_instance = programacao_financeira_dag()
