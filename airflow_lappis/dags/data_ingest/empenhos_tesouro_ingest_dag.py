from typing import Dict, Any, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
import pandas as pd
import io

# Configurações básicas da DAG
default_args = {
    "owner": "Davi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COLUMN_MAPPING = {
    0: "emissao_mes",
    1: "emissao_dia",
    2: "ne_ccor",
    3: "ne_num_processo",
    4: "ne_info_complementar",
    5: "ne_ccor_descricao",
    6: "doc_observacao",
    7: "natureza_despesa",
    8: "natureza_despesa_descricao",
    9: "ne_ccor_favorecido",
    10: "ne_ccor_favorecido_descricao",
    11: "ne_ccor_ano_emissao",
    12: "ptres",
    13: "fonte_recursos_detalhada",
    14: "fonte_recursos_detalhada_descricao",
    15: "despesas_empenhadas",
    16: "despesas_liquidadas",
    17: "despesas_pagas",
    18: "restos_a_pagar_inscritos",
    19: "restos_a_pagar_pagos",
}

EMAIL_SUBJECT = "notas_de_empenhos_a_partir_de_2024"
SKIPROWS = 9

# Configurações da DAG
with DAG(
    dag_id="email_empenhos_tesouro_ingest",
    default_args=default_args,
    description="Processa anexos dos empenhos vindo do email, formata e insere no db",
    schedule_interval="0 13 * * 1-6",
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "empenhos", "tesouro"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[str]:
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        try:
            logging.info("Iniciando o processamento dos emails...")
            csv_data = fetch_and_process_email(
                IMAP_SERVER,
                EMAIL,
                PASSWORD,
                SENDER_EMAIL,
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
        """
        Função para inserir os dados no banco de dados.
        Os dados do CSV são recuperados do XCom.
        """
        try:
            task_instance: Any = context["ti"]
            csv_data: Any = task_instance.xcom_pull(task_ids="process_emails")

            if not csv_data:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            df = pd.read_csv(io.StringIO(csv_data))
            df = df[df["ne_ccor_ano_emissao"].astype(str).str.startswith("20")]
            data = df.to_dict(orient="records")

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            unique_key = [
                "ne_ccor",
                "natureza_despesa",
                "doc_observacao",
                "ne_ccor_ano_emissao",
                "emissao_dia",
                "emissao_mes",
            ]

            db.insert_data(
                data,
                "empenhos_tesouro",
                conflict_fields=unique_key,
                primary_key=unique_key,
                schema="siafi",
            )
            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
            raise

    # Tarefa 1: Processar os e-mails e retornar CSV
    process_emails_task = PythonOperator(
        task_id="process_emails",
        python_callable=process_email_data,
        provide_context=True,
    )

    # Tarefa 2: Inserir os dados no banco de dados
    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_db,
        provide_context=True,
    )

    # Fluxo da DAG
    process_emails_task >> insert_to_db_task
