import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from cliente_postgres import ClientPostgresDB
from cliente_transferegov_fundo_a_fundo import ClienteTransfereGovBackend
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Caio Borges",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="minc_api_anexos_relatorios_dag",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["transfere_gov", "anexos", "raw"],
)
def minc_api_anexos_relatorios_dag() -> None:
    @task
    def fetch_anexos_relatorios() -> list[dict[str, Any]]:
        logging.info("[minc_api_anexos_relatorios_dag.py] Iniciando extração de anexos de relatórios")

        db = ClientPostgresDB(get_postgres_conn())
        ids_relatorios = db.get_id_relatorios_gestao(
            schema="transferegov_fundo_a_fundo", table_name="relatorios_gestao"
        )

        if not ids_relatorios:
            raise ValueError("[minc_api_anexos_relatorios_dag.py] Nenhum relatório de gestão encontrado")

        api = ClienteTransfereGovBackend()
        anexos_data: list[dict[str, Any]] = []

        for id_relatorio in ids_relatorios:
            logging.info(
                "[minc_api_anexos_relatorios_dag.py] Buscando anexos para relatório ID: %s",
                id_relatorio,
            )

            anexos_raw = api.get_anexos_relatorio(int(id_relatorio))

            if anexos_raw:
                for anexo in anexos_raw:
                    anexo["id_relatorio_gestao"] = id_relatorio
                    anexo["dt_ingest"] = datetime.now().isoformat()

                anexos_data.extend(anexos_raw)

                logging.info(
                    "[minc_api_anexos_relatorios_dag.py] Relatório %s: %d anexos encontrados",
                    id_relatorio,
                    len(anexos_raw),
                )
            else:
                logging.warning(
                    "[minc_api_anexos_relatorios_dag.py] Nenhum anexo encontrado para relatório ID: %s",
                    id_relatorio,
                )

        if not anexos_data:
            raise ValueError("[minc_api_anexos_relatorios_dag.py] Nenhum anexo foi extraído")

        logging.info(
            "[minc_api_anexos_relatorios_dag.py] Extração concluída com %s registros",
            len(anexos_data),
        )
        return anexos_data

    @task
    def load_anexos_to_postgres(anexos_data: list[dict[str, Any]]) -> None:
        logging.info("[minc_api_anexos_relatorios_dag.py] Iniciando carga no PostgreSQL")

        db = ClientPostgresDB(get_postgres_conn())
        db.insert_data(
            anexos_data,
            table_name="anexos_relatorios",
            primary_key=["id"],
            conflict_fields=["id"],
            schema="transferegov_fundo_a_fundo",
        )

        logging.info(
            "[minc_api_anexos_relatorios_dag.py] Carga concluída com %s registros",
            len(anexos_data),
        )

    trigger_download = TriggerDagRunOperator(
        task_id="trigger_download_anexos",
        trigger_dag_id="ingestao_anexos_transferegov",
        wait_for_completion=False,
    )

    load_anexos_to_postgres(fetch_anexos_relatorios()) >> trigger_download


minc_api_anexos_relatorios_dag()
