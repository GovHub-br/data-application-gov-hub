import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("documentos_habeis_especiais_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "documentos_especiais"],
)
def api_documentos_habeis_especiais_dag() -> None:
    """DAG para buscar e armazenar documentos hábeis especiais do Transfere Gov."""

    @task
    def fetch_and_store_documentos_habeis_especiais() -> None:
        logging.info(
            "[documentos_habeis_especiais_ingest_dag.py] Iniciando extração documentos "
            "hábeis especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Busca todos os documentos hábeis especiais com paginação automática
        documentos_data = api.get_all_documentos_habeis_especiais(page_size=1000)

        if documentos_data and len(documentos_data) > 0:
            # Adicionar dt_ingest a cada documento
            for documento in documentos_data:
                documento["dt_ingest"] = datetime.now().isoformat()

            # Inserir/atualizar dados no banco
            logging.info(
                f"[documentos_habeis_especiais_ingest_dag.py] Inserindo "
                f"{len(documentos_data)} documentos hábeis especiais no "
                f"schema transfere_gov"
            )
            db.insert_data(
                documentos_data,
                "documentos_habeis_especiais",
                conflict_fields=["id_dh"],
                primary_key=["id_dh"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[documentos_habeis_especiais_ingest_dag.py] Concluído. "
                f"Total de {len(documentos_data)} documentos hábeis especiais "
                f"inseridos/atualizados"
            )
        else:
            logging.warning(
                "[documentos_habeis_especiais_ingest_dag.py] Nenhum documento hábil "
                "especial encontrado"
            )

    fetch_and_store_documentos_habeis_especiais()


dag_instance = api_documentos_habeis_especiais_dag()
