import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

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
    dag_id="api_relatorios_gestao_dag",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minc", "transferegov", "relatorios", "raw"],
)
def api_relatorios_gestao_dag() -> None:
    @task
    def fetch_relatorios_gestao() -> list[dict[str, Any]]:
        logging.info("[api_relatorios_gestao_dag.py] Iniciando extração de relatórios de gestão")

        db = ClientPostgresDB(get_postgres_conn())
        ids_planos = db.get_id_planos_acao(schema="transferegov_fundo_a_fundo", table_name="raw_planos_acao")

        if not ids_planos:
            raise ValueError("[api_relatorios_gestao_dag.py] Nenhum plano de ação encontrado")

        api = ClienteTransfereGov()
        relatorios_data: list[dict[str, Any]] = []

        for id_plano in ids_planos:
            logging.info(
                "[api_relatorios_gestao_dag.py] Buscando relatórios para plano ID: %s",
                id_plano,
            )

            relatorios_raw = api.get_relatorios_by_plano_acao(int(id_plano))

            if relatorios_raw:
                relatorios_finais = [
                    r for r in relatorios_raw if r.get("tipo_relatorio_gestao") == "FINAL"
                ]

                for relatorio in relatorios_finais:
                    relatorio["dt_ingest"] = datetime.now().isoformat()

                relatorios_data.extend(relatorios_finais)

                logging.info(
                    "[api_relatorios_gestao_dag.py] Plano %s: %d relatórios FINAL encontrados",
                    id_plano,
                    len(relatorios_finais),
                )
            else:
                logging.warning(
                    "[api_relatorios_gestao_dag.py] Nenhum relatório encontrado para plano ID: %s",
                    id_plano,
                )

        if not relatorios_data:
            raise ValueError("[api_relatorios_gestao_dag.py] Nenhum relatório foi extraído")

        logging.info(
            "[api_relatorios_gestao_dag.py] Extração concluída com %s registros",
            len(relatorios_data),
        )
        return relatorios_data

    @task
    def load_relatorios_to_postgres(relatorios_data: list[dict[str, Any]]) -> None:
        logging.info("[api_relatorios_gestao_dag.py] Iniciando carga no PostgreSQL")

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            relatorios_data,
            table_name="relatorios_gestao",
            primary_key=["id_relatorio_gestao"],
            conflict_fields=["id_relatorio_gestao"],
            schema="transferegov_fundo_a_fundo",
        )

        logging.info(
            "[api_relatorios_gestao_dag.py] Carga concluída com %s registros",
            len(relatorios_data),
        )

    trigger_anexos = TriggerDagRunOperator(
        task_id="trigger_anexos",
        trigger_dag_id="api_anexos_relatorios_dag",
        wait_for_completion=False,
    )

    load_relatorios_to_postgres(fetch_relatorios_gestao()) >> trigger_anexos


api_relatorios_gestao_dag()
