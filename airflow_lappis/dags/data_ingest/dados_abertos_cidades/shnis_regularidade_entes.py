from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from helpers.utils import normalize_column_name
from postgres_helpers import get_postgres_conn
from cliente_snhis import ClienteSnhis
from cliente_postgres import ClientPostgresDB



def fetch_and_store_snhis_regularidade():
    logging.info("[SNHIS] Iniciando ingestão de regularidade dos entes")

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
    from datetime import datetime
    for row in data:
        row["dt_ingest"] = datetime.now().isoformat()

    logging.info(f"[SNHIS] Inserindo {len(data)} registros")
    # talvez o cnpj
    db.insert_data(
        data=data,
        table_name="dados_abertos_snhis_regularidade_entes",
        schema="dados_abertos_cidades",
        conflict_fields=["cnpj"],  
        primary_key=["cnpj"],
    )

    logging.info("[SNHIS] Finalizado com sucesso")


# definição da DAG
with DAG(
    dag_id="dados_abertos_snhis_regularidade_entes",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily", 
    catchup=False,
    tags=["snhis", "cidades", "dados_abertos"],
) as dag:

    task_ingest = PythonOperator(
        task_id="fetch_and_store_snhis_regularidade",
        python_callable=fetch_and_store_snhis_regularidade,
    )

    task_ingest