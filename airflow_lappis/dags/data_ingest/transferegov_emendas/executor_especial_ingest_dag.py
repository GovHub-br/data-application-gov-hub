import logging
from airflow.decorators import dag, task
from datetime import datetime
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Mateus e Gabriel",
        "retries": 0,
        # "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "planos_acao_especiais", "MIR"],
)
def api_executor_especial_dag() -> None:
    """DAG para buscar e armazenar executores especiais do Transfere Gov de forma massiva."""

    @task
    def fetch_and_store_executores_especiais() -> None:
        logging.info(
            "[executores_especiais_ingest_dag.py] Iniciando extração massiva de executores especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn("postgres_mir")
        db = ClientPostgresDB(postgres_conn_str)

        executores_data = api.get_all_executores_especiais(limit=1000)

        if executores_data and len(executores_data) > 0:
            timestamp_atual = datetime.now().isoformat()

            unique = {}
            for row in executores_data:
                key = (row["id_plano_acao"], row["id_executor"])
                unique[key] = row  # se já existir, substitui e mantém apenas 1

            executores_data = list(unique.values())

            for executor in executores_data:
                executor["dt_ingest"] = timestamp_atual

            logging.info(
                f"[executores_especiais_ingest_dag.py] Inserindo {len(executores_data)} "
                "executores no schema transferegov_emendas"
            )

            # Inserção/Update (Upsert)
            db.insert_data(
                executores_data,
                "executor_especial",
                conflict_fields=["id_plano_acao", "id_executor"],
                primary_key=["id_plano_acao", "id_executor"],
                schema="transfere_gov_emendas",
            )

            logging.info(
                f"[executores_especiais_ingest_dag.py] Concluído. Total de "
                f"{len(executores_data)} executores inseridos/atualizados"
            )
        else:
            logging.warning(
                "[executores_especiais_ingest_dag.py] Nenhum executor especial encontrado na API"
            )

    fetch_and_store_executores_especiais()


api_executor_especial_dag()
