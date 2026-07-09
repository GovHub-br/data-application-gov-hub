from typing import Dict, Any, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
import pandas as pd
import io
from schedule_loader import get_dynamic_schedule
from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

# Configuracoes basicas da DAG
default_args = {
    "owner": "Davi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
from datetime import datetime

from airflow import DAG
from email_ingest_dag_factory import build_email_ingest_dag

COLUMN_MAPPING = {
    0: "credor_codigo",
    1: "credor_nome",
    2: "dia_emissao",
    3: "mes_emissao",
    4: "ano_emissao",
    5: "emissao_ano",
    6: "mes_lancamento",
    7: "fonte_recursos_codigo",
    8: "fonte_recursos_descricao",
    9: "pi_codigo",
    10: "pi_descricao",
    11: "ptres",
    12: "natureza_codigo",
    13: "natureza_descricao",
    14: "processo",
    15: "valor",
    16: "observacao",
    17: "ne_ccor",
    18: "documento_habil",
    19: "item_informacao",
    20: "despesa_paga",
    21: "rp_processados",
    22: "rp_nao_processados",
    23: "pagamentos_totais",
}

EMAIL_SUBJECT = "bolsas_pagas_ipea"
SKIPROWS = 11


with DAG(
    dag_id="email_bolsas_pagas_tesouro_ingest",
    default_args=default_args,
dag: DAG = build_email_ingest_dag(
    dag_id="email_bolsas_pagas_tesouro_ingest",
    email_subject="bolsas_pagas_ipea",
    column_mapping=COLUMN_MAPPING,
    skiprows=11,
    table_name="bolsas_pagas",
    schedule_key="bolsas_pagas_ingest_dag",
    schema="siafi",
    owner="Davi",
    start_date=datetime(2023, 12, 1),
    tags=["email", "tesouro", "bolsas_pagas"],
    description=(
        "Processa anexos de bolsas pagas do Tesouro Gerencial recebidos por email "
        "e insere no banco"
    ),
    schedule_interval=get_dynamic_schedule("bolsas_pagas_ingest_dag"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "tesouro", "bolsas_pagas"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        email = creds["email"]
        password = creds["password"]
        imap_server = creds["imap_server"]
        sender_email = creds["sender_email"]

        try:
            logging.info("Iniciando o processamento dos emails...")
            csv_data = fetch_and_process_email(
                imap_server,
                email,
                password,
                sender_email,
                EMAIL_SUBJECT,
                COLUMN_MAPPING,
                skiprows=SKIPROWS,
            )

            if not csv_data:
                logging.warning("Nenhum e-mail encontrado com o assunto esperado.")
                return None

            logging.info(
                "CSV processado com sucesso. Dados encontrados: %s", len(csv_data)
            )
            return csv_data
        except Exception as e:
            logging.error("Erro no processamento dos emails: %s", str(e))
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        """Insere os dados processados no banco de dados."""
        try:
            task_instance: Any = context["ti"]
            csv_data: Any = task_instance.xcom_pull(task_ids="process_emails")

            if not csv_data:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            df = pd.read_csv(io.StringIO(csv_data))
            data = df.to_dict(orient="records")

            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)
            db.insert_data(data, "bolsas_pagas", schema="siafi")

            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
            raise

    def clean_duplicates(**context: Dict[str, Any]) -> None:
        """Remove duplicados da tabela siafi.bolsas_pagas."""
        try:
            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)
            db.remove_duplicates("bolsas_pagas", COLUMN_MAPPING, schema="siafi")
        except Exception as e:
            logging.error(f"Erro ao executar a limpeza de duplicados: {str(e)}")
            raise

    process_emails_task = PythonOperator(
        task_id="process_emails",
        python_callable=process_email_data,
        provide_context=True,
    )

    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_db,
        provide_context=True,
    )

    clean_duplicates_task = PythonOperator(
        task_id="clean_duplicates",
        python_callable=clean_duplicates,
        provide_context=True,
    )

    process_emails_task >> insert_to_db_task >> clean_duplicates_task
)
