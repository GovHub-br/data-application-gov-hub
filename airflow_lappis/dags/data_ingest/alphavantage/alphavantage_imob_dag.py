import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task

from postgres_helpers import get_postgres_conn
from cliente_postgres import ClientPostgresDB
from cliente_alphavantage import ClienteAlphaVantage

# Configurações padrão
DEFAULT_ARGS = {
    "owner": "Milena Rocha",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="ingestao_alphavantage_imob",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["infomoney", "alphavantage", "mercado_financeiro"],
)
def alphavantage_imob_dag() -> None:
    """
    DAG para extração de séries temporais do índice IMOB.SA
    via Alpha Vantage e carga no Postgres (schema infomoney).
    """

    @task
    def fetch_and_load_imob():
        logging.info("Iniciando extração Alpha Vantage (IMOB.SA)...")
        
        # 1. Configurações (Substitua pela sua chave real)
        API_KEY = "GITSIMPKGTDXDSZW" 
        SYMBOL = "IMOB.SA"
        
        # 2. Instanciando Clientes
        api = ClienteAlphaVantage(api_key=API_KEY)
        db = ClientPostgresDB(get_postgres_conn())
        
        # 3. Coleta de Dados
        dados_imob = api.get_daily_series(SYMBOL)
        
        if not dados_imob:
            logging.warning(f"Nenhum dado retornado para o símbolo {SYMBOL}. Verifique a API Key ou o limite de requisições.")
            return

        # 4. Enriquecimento com data de ingestão
        dt_ingest = datetime.now().isoformat()
        for item in dados_imob:
            item["dt_ingest"] = dt_ingest

        logging.info(f"Processados {len(dados_imob)} registros. Iniciando carga no banco...")

        # 5. Carga no Postgres
        # Nota: O schema 'infomoney' e a tabela 'acoes_imob' devem existir
        db.insert_data(
            dados_imob,
            table_name="acoes_imob",
            schema="infomoney",
            conflict_fields=["symbol", "data_pregao"],
            primary_key=["symbol", "data_pregao"]
        )
        
        logging.info("Carga finalizada com sucesso no schema infomoney.")

    # Execução da task
    fetch_and_load_imob()

# Inicialização da DAG
alphavantage_imob_dag()