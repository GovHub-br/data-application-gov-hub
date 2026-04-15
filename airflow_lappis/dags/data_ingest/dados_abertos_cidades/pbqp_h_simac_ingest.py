
from helpers.utils import normalize_column_name, process_and_clean_data
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_dados_gov_cidades import ClienteDadosGovCidades

DEFAULT_ARGS = {
    "owner": "Milena Rocha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="dados_abertos_pbqp_h_simac_ingest",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["cidades", "dados_abertos_pbqp_h_simac", "dados.gov.br"], # Vou padronizar: Ministério, tabela do dump, fonte do dado
)
def simac_dag():
    
    @task
    def ingest_simac():
        api = ClienteDadosGovCidades()
        db = ClientPostgresDB(get_postgres_conn())
        
        # Busca dado
        raw = api.get_empresas_qualificadas_simac()
        # Usa o helper comum
        data = process_and_clean_data(raw)
        db.insert_data(
        data,
        table_name="dados_abertos_pbqp_h_simac",
        schema="dados_abertos_cidades",
    )

    ingest_simac()

simac_dag()