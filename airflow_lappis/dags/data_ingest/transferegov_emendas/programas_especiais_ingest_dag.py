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
    tags=["transfere_gov_api", "programas_especiais"],
)
def api_programas_especiais_dag() -> None:
    """DAG para buscar e armazenar programas especiais do Transfere Gov."""

    @task
    def fetch_and_store_programas_especiais() -> None:
        logging.info(
            "[programas_especiais_ingest_dag.py] Iniciando extração programas especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Busca todos os programas especiais com paginação automática
        programas_data = api.get_all_programas_especiais(page_size=1000)

        if programas_data and len(programas_data) > 0:
            # Adicionar dt_ingest a cada programa
            for programa in programas_data:
                programa["dt_ingest"] = datetime.now().isoformat()

            # Inserir/atualizar dados no banco
            logging.info(
                f"[programas_especiais_ingest_dag.py] Inserindo {len(programas_data)} "
                "programas especiais no schema transfere_gov"
            )
            db.insert_data(
                programas_data,
                "programas_especiais",
                conflict_fields=["id_programa"],
                primary_key=["id_programa"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[programas_especiais_ingest_dag.py] Concluído. Total de "
                f"{len(programas_data)} programas especiais inseridos/atualizados"
            )
        else:
            logging.warning(
                "[programas_especiais_ingest_dag.py] Nenhum programa especial encontrado"
            )

    fetch_and_store_programas_especiais()


dag_instance = api_programas_especiais_dag()
