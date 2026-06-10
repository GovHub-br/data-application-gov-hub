import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 

from cliente_postgres import ClientPostgresDB
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Caio Borges",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="minc_api_programas_dag",
    schedule_interval=get_dynamic_schedule("minc_programas_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transfere_gov", "programas", "raw"],
)
def minc_api_programas_dag() -> None:
    @task
    def fetch_programas() -> list[dict[str, Any]]:
        logging.info("[minc_api_programas_dag.py] Iniciando extração de programas")
        ids_alvo = Variable.get(
            "transferegov_programas_ids",
            default_var=[46, 47],
            deserialize_json=True,
        )

        api = ClienteTransfereGov()
        programas_data: list[dict[str, Any]] = []

        for id_programa in ids_alvo:
            logging.info("[api_programas_dag.py] Buscando programa ID: %s", id_programa)
            programa = api.get_programa_by_id(int(id_programa))

            if programa:
                programa["dt_ingest"] = datetime.now().isoformat()
                programas_data.append(programa)
            else:
                logging.warning(
                    "[api_programas_dag.py] Programa não encontrado para ID: %s",
                    id_programa,
                )

        if not programas_data:
            raise ValueError("[minc_api_programas_dag.py] Nenhum programa foi extraído")

        logging.info(
            "[minc_api_programas_dag.py] Extração concluída com %s registros",
            len(programas_data),
        )
        return programas_data

    @task
    def load_programas_to_postgres(programas_data: list[dict[str, Any]]) -> None:
        logging.info("[minc_api_programas_dag.py] Iniciando carga no PostgreSQL")

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            programas_data,
            table_name="raw_programas",
            primary_key=["id_programa"],
            conflict_fields=["id_programa"],
            schema="transferegov_fundo_a_fundo",
        )

        logging.info(
            "[minc_api_programas_dag.py] Carga concluída com %s registros",
            len(programas_data),
        )

    carga_finalizada = load_programas_to_postgres(fetch_programas())

    trigger_planos_acao = TriggerDagRunOperator(
        task_id="trigger_planos_acao",
        trigger_dag_id="minc_api_planos_acao_dag",
        wait_for_completion=False,
    )

    carga_finalizada >>  trigger_planos_acao


minc_api_programas_dag()