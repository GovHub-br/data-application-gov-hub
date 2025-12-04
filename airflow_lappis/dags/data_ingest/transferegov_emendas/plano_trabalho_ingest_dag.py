import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow.models.dag import DAG
from airflow.decorators import dag, task
from schedule_loader import get_dynamic_schedule

from cliente_transferegov_emendas import ClienteTransfereGov
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB

default_args = {
    "owner": "Marcus Martins",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

TARGET_SCHEMA = "transferegov_emendas"
TARGET_TABLE_PLANO_ACAO = "plano_acao"
TARGET_TABLE_PLANO_TRABALHO = "plano_trabalho_especial"
BATCH_SIZE = 500  # Processa 500 IDs por Task do Airflow

def chunks(lst, n):
    """Função auxiliar para dividir uma lista em lotes de tamanho n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

@dag(
    dag_id="transferegov_emendas_plano_trabalho_especial_ingest_dag",
    description="Ingestão de Planos de Trabalho Especial em Lotes.",
    schedule=get_dynamic_schedule("transferegov_plano_trabalho_especial_ingest_dag"),
    start_date=datetime(2025, 12, 2),
    catchup=False,
    default_args=default_args,
    tags=["transfere_gov_api", "programas_especiais", "transferegov", "plano_trabalho_especial"],
)


def ingest_transferegov_plano_trabalho_dag() -> DAG:

    @task
    def get_planos_acao_batches() -> List[List[int]]:
        """
        Recupera todos os IDs e os divide em lotes (listas de listas).
        """
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        
        logging.info(f"Buscando IDs em {TARGET_SCHEMA}.{TARGET_TABLE_PLANO_ACAO}...")

        all_ids = db.get_all_ids(
            schema=TARGET_SCHEMA,
            table_name=TARGET_TABLE_PLANO_ACAO,
            id_column="id_plano_acao"
        )
        
        if not all_ids:
            logging.warning("Nenhum ID encontrado.")
            return []

        # Convertendo para lista de lotes para o Dynamic Task Mapping não explodir
        batched_ids = list(chunks(all_ids, BATCH_SIZE))
        
        logging.info(f"Total de IDs: {len(all_ids)}. Divididos em {len(batched_ids)} lotes de {BATCH_SIZE}.")
        return batched_ids

    @task
    def fetch_plano_trabalho_batch(ids_batch: List[int]) -> List[Dict[str, Any]]:
        """
        Processa um lote de IDs. Faz o loop interno.
        """
        cliente = ClienteTransfereGov()
        resultados_lote = []
        
        for id_plano_acao in ids_batch:
            dados = cliente.get_all_planos_trabalho_especial_by_plano_acao(id_plano_acao)
            if dados:
                resultados_lote.extend(dados)
        
        return resultados_lote

    @task
    def save_to_postgres(dados_planos: List[List[Dict]]) -> None:
        """
        Recebe a lista de resultados dos lotes e salva.
        """
        # Flatten a lista de listas (Lotes -> Lista Única)
        todos_planos = [item for lote in dados_planos for item in lote]

        if not todos_planos:
            logging.info("Nenhum plano de trabalho especial encontrado em nenhum dos lotes.")
            return

        logging.info(f"Total de registros para inserir: {len(todos_planos)}")

        dt_ingest_atual = datetime.now().isoformat()
        for plano in todos_planos:
            plano["dt_ingest"] = dt_ingest_atual

        try:
            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)
            
            db.insert_data(
                TARGET_TABLE_PLANO_TRABALHO,
                todos_planos,
                schema=TARGET_SCHEMA,
                conflict_fields=["id_plano_trabalho"],
                primary_key=["id_plano_trabalho"]
            )
            logging.info("Inserção concluída com sucesso.")
            
        except Exception as e:
            logging.error(f"Erro ao salvar dados no Postgres: {e}")
            raise

    lotes_ids = get_planos_acao_batches()
    
    dados_extraidos = fetch_plano_trabalho_batch.expand(ids_batch=lotes_ids)

    save_to_postgres(dados_planos=dados_extraidos)


ingest_transferegov_plano_trabalho_dag()
