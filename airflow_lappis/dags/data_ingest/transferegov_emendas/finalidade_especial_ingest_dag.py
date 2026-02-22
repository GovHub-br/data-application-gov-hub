import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from schedule_loader import get_dynamic_schedule
from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB


@dag(
    schedule_interval=get_dynamic_schedule("finalidade_especial_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Tiago",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "finalidade_especial"],
)
def api_finalidade_especial_dag() -> None:
    """DAG para buscar e armazenar finalidades especiais do Transfere Gov."""

    @task
    def fetch_and_store_finalidade_especial() -> None:
        logging.info(
            "[finalidade_especial_ingest_dag.py] Iniciando extração finalidades especiais"
        )

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        # Busca todas as finalidades especiais com paginação automática
        finalidades_data = api.get_all_finalidades_especiais(page_size=1000)

        if finalidades_data and len(finalidades_data) > 0:
            # Adicionar dt_ingest a cada documento
            for documento in finalidades_data:
                documento["dt_ingest"] = datetime.now().isoformat()

            # Inserir/atualizar dados no banco
            logging.info(
                f"[finalidade_especial_ingest_dag.py] Inserindo "
                f"{len(finalidades_data)} finalidades especiais no "
                f"schema transfere_gov"
            )
            db.insert_data(
                finalidades_data,
                "finalidades_especiais",
                conflict_fields=["id_executor", "cd_area_politica_publica_tipo_pt", 
                                 "area_politica_publica_pt"],
                primary_key=["id_executor", "cd_area_politica_publica_tipo_pt",
                             "area_politica_publica_pt"],
                schema="transferegov_emendas",
            )

            logging.info(
                f"[finalidade_especial_ingest_dag.py] Concluído. "
                f"Total de {len(finalidades_data)} finalidades especiais "
                f"inseridas/atualizadas"
            )
        else:
            logging.warning(
                "[finalidade_especial_ingest_dag.py] Nenhuma finalidade especial "
                "encontrada"
            )

    fetch_and_store_finalidade_especial()

api_finalidade_especial_dag()
