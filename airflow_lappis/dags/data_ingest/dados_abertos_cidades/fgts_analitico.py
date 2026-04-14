from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import tempfile
import pandas as pd
from helpers.utils import normalize_column_name
from postgres_helpers import get_postgres_conn
from cliente_snhis import ClienteSnhis
from cliente_postgres import ClientPostgresDB

def fetch_and_store_fgts_analitico():
    logging.info("[FGTS Analítico] Iniciando extração e ingestão")

    api = ClienteSnhis()
    db = ClientPostgresDB(get_postgres_conn())

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Baixa e extrai o arquivo usando o método que criamos no cliente
        # Note: o método download_and_extract_fgts retorna o path do CSV/XLS
        file_path = api.download_and_extract_fgts(tmpdir)
        
        logging.info(f"[FGTS Analítico] Lendo arquivo extraído: {file_path}")
        
        # 2. Carrega os dados (usando ; e latin-1 que é o padrão desse arquivo)
        if file_path.lower().endswith(".csv"):
            df = pd.read_csv(file_path, sep=";", encoding="latin-1", on_bad_lines='skip')
        else:
            df = pd.read_excel(file_path)

        # 3. Normaliza as colunas e converte para lista de dicionários
        # Aplicamos o seu helper de normalização em cada chave
        data = [
            {normalize_column_name(k): v for k, v in row.items()}
            for row in df.to_dict(orient="records")
        ]

    if not data:
        logging.warning("[FGTS Analítico] Nenhum dado encontrado no arquivo")
        return

    # 4. Adiciona metadados de ingestão
    dt_now = datetime.now().isoformat()
    for row in data:
        row["dt_ingest"] = dt_now

    logging.info(f"[FGTS Analítico] Inserindo {len(data)} registros no Postgres")

    # 5. Inserção no Banco
    # Nota: Verifique se o arquivo analítico possui uma chave única clara. 
    # Caso não tenha, remova conflict_fields ou use um hash das colunas.
    db.insert_data(
        data=data,
        table_name="mcmv_fgts_analitico",
        schema="dados_abertos_cidades",
        conflict_fields=None, # Arquivos analíticos muitas vezes não têm PK óbvia
        primary_key=None,
    )

    logging.info("[FGTS Analítico] Finalizado com sucesso")


# Definição da DAG
with DAG(
    dag_id="dados_abertos_mcmv_fgts_analitico",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@monthly", # Geralmente atualizado mensalmente
    catchup=False,
    tags=["fgts", "mcmv", "cidades", "dados_abertos"],
) as dag:

    task_ingest_fgts = PythonOperator(
        task_id="fetch_and_store_fgts_analitico",
        python_callable=fetch_and_store_fgts_analitico,
    )