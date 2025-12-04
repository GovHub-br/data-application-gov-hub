import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule

from postgres_helpers import get_postgres_conn
from cliente_transferegov_emendas import ClienteTransfereGov
from cliente_postgres import ClientPostgresDB

TARGET_SCHEMA = "transferegov_emendas"
TABLE_PROGRAMAS = "programa_especial" 
TABLE_PLANOS = "plano_acao"

@dag(
    dag_id="transferegov_emendas_planos_acao_especiais_ingest_dag",
    schedule=get_dynamic_schedule("planos_acao_especiais_ingest_dag"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "owner": "Davi, Mateus, Marcus",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["transfere_gov_api", "planos_acao_especiais"],
)
def api_planos_acao_especiais_dag() -> None:
    """DAG para buscar e armazenar planos de ação especiais do Transfere Gov."""

    @task
    def fetch_and_store_planos_acao_especiais() -> None:
        logging.info("Iniciando extração de planos de ação especiais")

        api = ClienteTransfereGov()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        try:
            query = f"SELECT DISTINCT id_programa FROM {TARGET_SCHEMA}.{TABLE_PROGRAMAS}"
            logging.info(f"Buscando IDs de programas em: {query}")
            programas_ids = db.execute_query(query)
        except Exception as e:
            logging.warning(f"Erro ao buscar programas: {e}")
            return

        if not programas_ids:
            logging.warning("Nenhum programa encontrado na tabela base. Execute a ingestão de Programas primeiro.")
            return

        total_planos = 0
        
        for row in programas_ids:
            id_programa = row[0]
            
            planos_data = api.get_all_planos_acao_especiais_by_programa(id_programa)

            if planos_data:
                for plano in planos_data:
                    plano["dt_ingest"] = datetime.now().isoformat()

                db.insert_data(
                    planos_data,
                    TABLE_PLANOS,
                    conflict_fields=["id_plano_acao"],
                    primary_key=["id_plano_acao"],
                    schema=TARGET_SCHEMA,
                )
                total_planos += len(planos_data)

        logging.info(f"Concluído. Total: {total_planos} planos de ação inseridos/atualizados")

    fetch_and_store_planos_acao_especiais()


dag_instance = api_planos_acao_especiais_dag()
