import io
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule

default_args = {
    "owner": "Lucas",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COLUMN_MAPPING = {
    0: "emissao_mes",
    1: "emissao_dia",
    2: "programa_governo_numero",
    3: "programa_governo_nome",
    4: "acao_governo_codigo",
    5: "acao_governo_nome",
    6: "autor_emendas_orcamento_codigo",
    7: "autor_emendas_orcamento_nome",
    8: "uf_codigo",
    9: "uf_nome",
    10: "municipio",
    11: "ne_ccor",
    12: "ne_num_processo",
    13: "ne_info_complementar",
    14: "ne_ccor_descricao",
    15: "doc_observacao",
    16: "grupo_despesa_codigo",
    17: "grupo_despesa_nome",
    18: "natureza_despesa_codigo",
    19: "natureza_despesa_nome",
    20: "modalidade_aplicacao_codigo",
    21: "modalidade_aplicacao_nome",
    22: "ne_ccor_favorecido_codigo",
    23: "ne_ccor_favorecido_nome",
    24: "ne_ccor_ano_emissao",
    25: "ptres",
    26: "fonte_recursos_detalhada",
    27: "fonte_recursos_detalhada_descricao",
    28: "restos_a_pagar_inscritos",
    29: "restos_a_pagar_pagos",
}

EMAIL_SUBJECT = "notas_empenho_emendas_parlamentares_mcid"
SKIPROWS = 9

with DAG(
    dag_id="empenho_emendas_parlamentares_ingest_dag",
    default_args=default_args,
    description="Processa e ingere dados de empenho de emendas parlamentares do MCID",
    schedule_interval=get_dynamic_schedule("empenho_emendas_parlamentares_ingest_dag"),
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=["email", "empenhos", "tesouro", "emendas"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        try:
            logging.info("Iniciando o processamento dos emails")
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
                raise AirflowSkipException("Nenhum e-mail encontrado. Task ignorada.")

            logging.info(
                "CSV processado com sucesso. Dados encontrados: %s", len(csv_data)
            )
            return csv_data
        except Exception as e:
            logging.error("Erro no processamento dos emails: %s", str(e))
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        try:
            task_instance: Any = context["ti"]
            csv_data: Any = task_instance.xcom_pull(task_ids="process_emails")

            if not csv_data:
                logging.warning("Nenhum dado para inserir no banco.")
                raise AirflowSkipException(
                    "Nenhum dado foi encontrado para inserção no BD"
                )

            df = pd.read_csv(io.StringIO(csv_data), skiprows=[1, 2, 3])
            df = df[df["ne_ccor_ano_emissao"].astype(str).str.startswith("20")]
            data = df.to_dict(orient="records")

            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            unique_key = [
                "ne_ccor",
                "emissao_dia",
                "emissao_mes",
                "programa_governo_numero",
                "programa_governo_nome",
                "ne_num_processo",
            ]

            db.insert_data(
                data,
                "empenho_emendas_parlamentares",
                conflict_fields=unique_key,
                primary_key=unique_key,
                schema="siafi",
            )
            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
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

    process_emails_task >> insert_to_db_task
