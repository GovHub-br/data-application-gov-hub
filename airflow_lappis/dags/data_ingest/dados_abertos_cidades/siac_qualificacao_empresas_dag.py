import logging
from helpers.utils import normalize_column_name
import pandas as pd # Import necessário para a limpeza
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_siac import ClienteSiac
from cliente_snhis import ClienteSnhis

DEFAULT_ARGS = {
    "owner": "Milena Rocha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pbqp_h_ingestion_dag",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ministerio_cidades", "pbqp-h", "siac", "simac"],
)
def pbqp_h_ingestion_dag() -> None:

    
    # def normalize_column_name(col: str) -> str:

    #     col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
    #     col = col.lower()
    #     col = re.sub(r'[^a-z0-9]+', '_', col)
    #     col = re.sub(r'_+', '_', col)
    #     col = col.strip('_')
        
    #     return col

    def process_ingestion(tipo: str, table_name: str):
        logging.info(f"Iniciando extração do {tipo}")
        
        api = ClienteSiac()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        if tipo == "SiAC":
            raw_data = api.get_empresas_certificadas_siac()
        else:
            raw_data = api.get_empresas_qualificadas_simac()

        if raw_data:
            df = pd.DataFrame(raw_data)

            # 1. Converte colunas datetime -> string (ISO) ou None
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)

            # 2. Converte tudo para object
            df = df.astype(object)

            # 3. Substitui QUALQUER tipo de nulo por None
            df = df.where(pd.notna(df), None)

            # 4. Remove strings problemáticas
            df = df.replace(['NaT', 'nan', 'NaN', 'None'], None)

            df = df.astype(str)

            df = df.replace({
                'NaT': None,
                'nan': None,
                'NaN': None,
                'None': None
            })  

            df.columns = [normalize_column_name(col) for col in df.columns]
            empresas_data = df.to_dict(orient="records")
            
        

            dt_ingest = datetime.now().isoformat()
            for item in empresas_data:
                item["dt_ingest"] = dt_ingest

            logging.info(f"Inserindo {len(empresas_data)} registros limpos em {table_name}")

            db.insert_data(
                empresas_data,
                table_name=table_name,
                conflict_fields=None,
                primary_key=None,
                schema="dados_abertos_cidades",
            )
        else:
            logging.warning(f"Nenhum dado encontrado para {tipo}")

    @task
    def fetch_siac_certificadas():
        process_ingestion(tipo="SiAC", table_name="dados_abertos_pbqp_h_siac")

    @task
    def fetch_simac_qualificadas():
        process_ingestion(tipo="SiMaC", table_name="dados_abertos_pbqp_h_simac")

    @task
    def fetch_snhis_regularidade():
        logging.info("Iniciando extração do SNHIS Regularidade")
        
        api = ClienteSnhis() # Instancia o novo cliente
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)

        raw_data = api.get_regularidade_entes()

        if raw_data:
            df = pd.DataFrame(raw_data)
            
            # Reutiliza sua lógica de normalização de colunas
            df.columns = [normalize_column_name(col) for col in df.columns]
            
            # Converte para dict para inserção
            data_to_insert = df.to_dict(orient="records")
            
            dt_ingest = datetime.now().isoformat()
            for item in data_to_insert:
                item["dt_ingest"] = dt_ingest

            logging.info(f"Inserindo {len(data_to_insert)} registros do SNHIS")

            db.insert_data(
                data_to_insert,
                table_name="dados_abertos_snhis_regularidade",
                conflict_fields=None,
                primary_key=None,
                schema="dados_abertos_cidades",
            )
    @task
    def fetch_fgts_analitico():
        logging.info("Iniciando extração FGTS Analítico")
        api = ClienteSnhis()
        postgres_conn_str = get_postgres_conn()
        db = ClientPostgresDB(postgres_conn_str)
        
      
        raw_data = api.get_fgts_analitico()

        if raw_data:
            df = pd.DataFrame(raw_data)
            # Reutiliza sua lógica de normalização e inserção...
            db.insert_data(
                df,
                table_name="dados_abertos_fgts_analitico",
                conflict_fields=None,
                primary_key=None,
                schema="dados_abertos_cidades",
            )

    fetch_siac_certificadas()
    fetch_simac_qualificadas()
    fetch_snhis_regularidade() 
    fetch_fgts_analitico()

pbqp_h_ingestion_dag()