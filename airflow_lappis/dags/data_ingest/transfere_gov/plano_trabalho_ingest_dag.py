import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import psycopg2 

from airflow.models.dag import DAG
from airflow.decorators import dag, task

from cliente_transferegov_emendas import ClienteTransfereGov
from postgres_helpers import get_postgres_conn

RAW_LAYER_PATH = Path("/opt/airflow/data/raw/transferegov/plano_trabalho_especial")

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
    description="Ingestão de Planos de Trabalho Especial (TransfereGov) baseada em IDs de Planos de Ação.",
    schedule="@daily",
    start_date=datetime(2025, 12, 2),
    catchup=False,
    default_args=default_args,
    tags=["transferegov", "emendas", "plano_trabalho_especial"],
)

def ingest_transferegov_plano_trabalho_dag() -> DAG:  # type: ignore[return]

    @task
    def get_planos_acao_ids() -> List[int]:
        """
        Tenta recuperar IDs do Postgres. 
        Se a tabela não existir (ambiente dev), retorna IDs simulados para testar a API.
        """
        logging.info("Iniciando busca de IDs de Planos de Ação.")

        target_schema = "transferegov"
        target_table = "plano_acao"
        full_table_name = f"{target_schema}.{target_table}"
        postgres_conn_str = get_postgres_conn()

        try:
            with psycopg2.connect(postgres_conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT id_plano_acao FROM {full_table_name} LIMIT 100")
                    rows = cursor.fetchall()
                    
                    ids = [row[0] for row in rows]
                    return [i for i in ids if i is not None]

        except Exception as e:
            logging.warning(f"Não foi possível ler do banco ({e}). Usando IDs simulados.")
            return [9060, 9061, 1234]

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
    def save_to_raw(dados_planos: List[List[Dict]], execution_date: str) -> str:
        """
        Recebe uma lista de listas (resultado do processamento paralelo),
        consolida (flatten) e salva no Data Lake.
        """

        todos_planos = [item for sublist in dados_planos for item in sublist]

        if not todos_planos:
            logging.info("Nenhum plano de trabalho especial encontrado.")
            return "N/A"

        output_path = RAW_LAYER_PATH / f"extraction_date={execution_date}"
        output_path.mkdir(parents=True, exist_ok=True)
        output_file = output_path / "plano_trabalho_especial.json"

        logging.info(f"Salvando {len(todos_planos)} registros em: {output_file}")

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(todos_planos, f, ensure_ascii=False, indent=2)

        return str(output_file)

    ids_acao = get_planos_acao_ids()

    dados_extraidos = fetch_plano_trabalho_especial.expand(id_plano_acao=ids_acao)

    save_to_raw(dados_planos=dados_extraidos, execution_date="{{ ds }}")


ingest_transferegov_plano_trabalho_dag()