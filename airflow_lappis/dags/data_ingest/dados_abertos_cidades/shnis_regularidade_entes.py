from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from helpers.utils import normalize_column_name
from postgres_helpers import get_postgres_conn
from cliente_snhis import ClienteSnhis
from cliente_postgres import ClientPostgresDB
from airflow.decorators import dag, task





DEFAULT_ARGS = {
    "owner": "Milena Rocha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dados_abertos_snhis_regularidade_entes_ingest",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cidades", "dados_abertos_snhis_regularidade_entes", "dados.gov.br"], # Vou padronizar: Ministério, tabela do dump, fonte do dado
)
def snhis_dag():
    
    @task
    def ingest_snhis():
        
        api = ClienteSnhis()
        db = ClientPostgresDB(get_postgres_conn())
        data = api.get_regularidade_entes()
        data = [
        {normalize_column_name(k): v for k, v in row.items()}
        for row in data
    ]
        
        if not data:
            logging.warning("[SNHIS] Nenhum dado retornado")
            return

        # adiciona coluna de ingestão

        # for row in data:
        #     row["dt_ingest"] = datetime.now().isoformat()

        logging.info(f"[SNHIS] Inserindo {len(data)} registros")
        # talvez o cnpj
        db.insert_data(
            data=data,
            table_name="dados_abertos_snhis_regularidade_entes",
            schema="dados_abertos_cidades",
            conflict_fields=["cnpj"],  
            primary_key=["cnpj"],
        )

    ingest_snhis()

snhis_dag()