import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import psycopg2 

from airflow.models.dag import DAG
from airflow.decorators import dag, task

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


@dag(
    dag_id="ingest_transferegov_plano_trabalho_especial",
    description="Ingestão de Planos de Trabalho Especial (TransfereGov) diretamente no Postgres.",
    schedule="@daily",
    start_date=datetime(2025, 12, 2),
    catchup=False,
    default_args=default_args,
    tags=["transferegov", "emendas", "plano_trabalho_especial", "postgres"],
)
def ingest_transferegov_plano_trabalho_dag() -> DAG:  # type: ignore[return]

    @task
    def get_planos_acao_ids() -> List[int]:
        """
        Recupera os IDs reais dos Planos de Ação armazenados no banco de dados Postgres.
        A task falhará se não conseguir conectar ao banco ou se a tabela não existir.
        """
        logging.info("Iniciando busca de IDs de Planos de Ação.")

        target_schema = "transferegov_emendas"
        target_table = "plano_acao"
        full_table_name = f"{target_schema}.{target_table}"
        
        postgres_conn_str = get_postgres_conn()

        with psycopg2.connect(postgres_conn_str) as conn:
            with conn.cursor() as cursor:
                logging.info(f"Executando query em: {full_table_name}")
                
                cursor.execute(f"SELECT id_plano_acao FROM {full_table_name}")
                rows = cursor.fetchall()
                
                if not rows:
                    logging.warning("Consulta realizada com sucesso, mas a tabela está vazia.")
                    return []
                
                ids = [row[0] for row in rows]
                clean_ids = [i for i in ids if i is not None]
                
                logging.info(f"Sucesso! {len(clean_ids)} IDs recuperados do banco.")
                return clean_ids

    @task
    def fetch_plano_trabalho_especial(id_plano_acao: int) -> List[Dict[str, Any]]:
        """
        Consulta a API para buscar planos de trabalho especiais.
        A lógica de paginação (loop) está encapsulada no cliente.
        """
        cliente = ClienteTransfereGov()
        dados = cliente.get_all_planos_trabalho_especial_by_plano_acao(id_plano_acao)
        return dados

    @task
    def save_to_postgres(dados_planos: List[List[Dict]]) -> None:
        """
        Recebe uma lista de listas, consolida, adiciona metadados e insere no Postgres.
        """
        todos_planos = [item for sublist in dados_planos for item in sublist]

        if not todos_planos:
            logging.info("Nenhum plano de trabalho especial encontrado.")
            return

        dt_ingest_atual = datetime.now().isoformat()
        for plano in todos_planos:
            plano["dt_ingest"] = dt_ingest_atual

        logging.info(f"Preparando inserção de {len(todos_planos)} registros no Postgres.")

        try:
            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)
            
            db.insert_data(
                data=todos_planos,
                table="plano_trabalho_especial",
                schema="transferegov",
                conflict_fields=["id_plano_trabalho"],
                primary_key=["id_plano_trabalho"]
            )
            logging.info("Inserção concluída com sucesso.")
            
        except Exception as e:
            logging.error(f"Erro ao salvar dados no Postgres: {e}")
            raise

    ids_acao = get_planos_acao_ids()

    dados_extraidos = fetch_plano_trabalho_especial.expand(id_plano_acao=ids_acao)

    save_to_postgres(dados_planos=dados_extraidos)


ingest_transferegov_plano_trabalho_dag()