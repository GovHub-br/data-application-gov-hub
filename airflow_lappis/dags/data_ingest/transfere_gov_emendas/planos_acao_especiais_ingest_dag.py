import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi e Mateus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "planos_acao_especiais"],
)
def api_planos_acao_especiais_dag() -> None:
    """DAG para buscar e armazenar planos de ação especiais do Transfere Gov."""

    @task
    def fetch_and_store_planos_acao_especiais() -> None:
        logging.info(
            "[planos_acao_especiais_ingest_dag.py] Iniciando extração de "
            "planos de ação especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Buscar IDs dos programas especiais
        query = "SELECT DISTINCT id_programa FROM transfere_gov.programas_especiais"
        programas_ids = db.execute_query(query)

        if not programas_ids:
            logging.warning(
                "[planos_acao_especiais_ingest_dag.py] Nenhum programa encontrado"
            )
            return

        total_planos = 0
        for (id_programa,) in programas_ids:
            logging.info(
                f"[planos_acao_especiais_ingest_dag.py] Buscando planos de ação "
                f"para programa {id_programa}"
            )

            planos_data = api.get_all_planos_acao_especiais_by_programa(id_programa)

            if planos_data:
                for plano in planos_data:
                    plano["dt_ingest"] = datetime.now().isoformat()

                db.insert_data(
                    planos_data,
                    "planos_acao_especiais",
                    conflict_fields=["id_plano_acao"],
                    primary_key=["id_plano_acao"],
                    schema="transfere_gov_emendas",
                )
                total_planos += len(planos_data)

        logging.info(
            f"[planos_acao_especiais_ingest_dag.py] Concluído. "
            f"Total: {total_planos} planos de ação inseridos/atualizados"
        )

    fetch_and_store_planos_acao_especiais()


dag_instance = api_planos_acao_especiais_dag()
