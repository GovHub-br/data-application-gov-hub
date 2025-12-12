import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("empenhos_especiais_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Leonardo e Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "empenhos_especiais"],
)
def api_empenhos_especiais_dag() -> None:
    """DAG para buscar e armazenar empenhos especiais do Transfere Gov."""

    @task
    def fetch_and_store_empenhos_especiais() -> None:
        logging.info(
            "[empenhos_especiais_ingest_dag.py] Iniciando extração de "
            "empenhos especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Busca todos os documentos hábeis especiais com paginação automática
        documentos_data = api.get_all_empenhos_especiais(page_size=1000)

        if documentos_data and len(documentos_data) > 0:
            # Adicionar dt_ingest a cada documento
            for documento in documentos_data:
                documento["dt_ingest"] = datetime.now().isoformat()

            # Inserir/atualizar dados no banco
            logging.info(
                f"[empenhos_especiais_ingest_dag.py] Inserindo {len(documentos_data)} "
                "empenhos especiais no schema transfere_gov"
            )
            db.insert_data(
                documentos_data,
                "empenhos_especiais",
                conflict_fields=["id_empenho"],
                primary_key=["id_empenho"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[empenhos_especiais_ingest_dag.py] Concluído. Total de "
                f"{len(documentos_data)} empenhos especiais inseridos/atualizados"
            )
        else:
            logging.warning(
                "[empenhos_especiais_ingest_dag.py] Nenhum empenho " \
                "especial encontrado"
            )

    fetch_and_store_empenhos_especiais()


dag_instance = api_empenhos_especiais_dag()