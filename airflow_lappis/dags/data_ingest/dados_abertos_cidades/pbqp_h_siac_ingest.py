
from helpers.utils import normalize_column_name, process_and_clean_data
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_dados_gov_cidades import ClienteDadosGovCidades
from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "Milena Rocha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dados_abertos_pbqp_h_siac_ingest",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cidades", "dados_abertos_pbqp_h_siac", "dados.gov.br"], # Vou padronizar: Ministério, tabela do dump, fonte do dado
)
def siac_dag():
    
    @task
    def ingest_siac():
        api = ClienteDadosGovCidades()
        db = ClientPostgresDB(get_postgres_conn())
        
        # Busca dado
        raw = api.get_empresas_certificadas_siac()
        # Usa o helper comum
        data = process_and_clean_data(raw)
        db.insert_data(
        data,
        table_name="dados_abertos_pbqp_h_siac",
        schema="dados_abertos_cidades",
    )

    ingest_siac()

siac_dag()