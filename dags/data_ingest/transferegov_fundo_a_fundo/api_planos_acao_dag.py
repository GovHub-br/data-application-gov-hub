import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.sdk import Variable
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from cliente_postgres import ClientPostgresDB
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGov
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Wallyson Souza",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="api_planos_acao_dag",
    schedule=get_dynamic_schedule("api_planos_acao_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "planos_acao", "raw"],
)
def api_planos_acao_dag() -> None:
    @task
    def fetch_planos_acao() -> list[dict[str, Any]]:
        logging.info("[api_planos_acao_dag.py] Iniciando extração de planos de ação")

        ids_alvo = Variable.get(
            "transferegov_programas_ids",
            default=[46, 47],
            deserialize_json=True,
        )

        api = ClienteTransfereGov()
        planos_data: list[dict[str, Any]] = []

        for id_programa in ids_alvo:
            logging.info(
                "[api_planos_acao_dag.py] Buscando planos de ação para programa ID: %s",
                id_programa,
            )
            planos = api.get_planos_acao_by_programa(int(id_programa))

            if planos:
                for plano in planos:
                    plano["dt_ingest"] = datetime.now().isoformat()

                planos_data.extend(planos)
                logging.info(
                    "[api_planos_acao_dag.py] Programa %s: %d planos encontrados",
                    id_programa,
                    len(planos),
                )
            else:
                logging.warning(
                    "[api_planos_acao_dag.py] Nenhum plano encontrado para programa ID: %s",
                    id_programa,
                )

        if not planos_data:
            raise ValueError("[api_planos_acao_dag.py] Nenhum plano de ação foi extraído")

        logging.info(
            "[api_planos_acao_dag.py] Extração concluída com %s registros",
            len(planos_data),
        )
        return planos_data

    @task
    def load_planos_to_postgres(planos_data: list[dict[str, Any]]) -> None:
        logging.info("[api_planos_acao_dag.py] Iniciando carga no PostgreSQL")

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            planos_data,
            table_name="raw_planos_acao",
            primary_key=["id_plano_acao"],
            conflict_fields=["id_plano_acao"],
            schema="transferegov_fundo_a_fundo",
        )

        logging.info(
            "[api_planos_acao_dag.py] Carga concluída com %s registros",
            len(planos_data),
        )

    trigger_relatorios = TriggerDagRunOperator(
        task_id="trigger_relatorios",
        trigger_dag_id="api_relatorios_gestao_dag",
        wait_for_completion=False,
    )

    load_planos_to_postgres(fetch_planos_acao()) >> trigger_relatorios


api_planos_acao_dag()
