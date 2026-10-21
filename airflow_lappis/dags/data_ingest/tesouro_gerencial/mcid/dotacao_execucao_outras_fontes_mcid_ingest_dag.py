import io
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import cliente_email
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
    0: "unidade_orcamentaria_codigo",
    1: "unidade_orcamentaria_nome",
    2: "acao_governo_codigo",
    3: "acao_governo_nome",
    4: "programa_governo_codigo",
    5: "programa_governo_nome",
    6: "plano_orcamentario_codigo",
    7: "plano_orcamentario_funcao",
    8: "plano_orcamentario_subfuncao",
    9: "plano_orcamentario_programa",
    10: "plano_orcamentario_acao",
    11: "plano_orcamentario_medida",
    12: "plano_orcamentario_descricao",
    13: "elemento_despesa_codigo",
    14: "elemento_despesa_nome",
    15: "orgao_uge_codigo",
    16: "orgao_uge_nome",
    17: "uge_matriz_filial",
    18: "ug_executora_codigo",
    19: "ug_executora_nome",
    20: "fixacao_despesa_loa",
    21: "dotacao_inicial",
    22: "dotacao_atualizada",
    23: "credito_disponivel",
    24: "despesas_empenhadas",
    25: "despesas_empenhadas_a_liquidar",
    26: "despesas_liquidadas_a_pagar",
    27: "despesas_pagas",
    28: "restos_a_pagar_inscritos",
    29: "restos_a_pagar_pagos",
}

EMAIL_SUBJECT = "dotacao_execucao_outras_fontes_mcid"
SKIPROWS = 12


def _patched_format_csv(
    csv_data: str,
    column_mapping: Optional[Dict[int, str]],
    skiprows: int,
) -> pd.DataFrame:
    """Substitui o format_csv do cliente_email com suporte a UTF-16 e TSV."""
    # Decodifica UTF-16 se ainda vier como bytes
    if isinstance(csv_data, bytes):
        csv_data = csv_data.decode("utf-16")

    if column_mapping:
        df = pd.read_csv(
            io.StringIO(csv_data),
            skiprows=skiprows,
            header=None,
            sep="\t",
            engine="python",
            on_bad_lines="skip",
        )
        column_names: List[str] = [
            column_mapping.get(i, f"col_{i}") for i in range(len(df.columns))
        ]
        df.columns = pd.Index(column_names)
    else:
        df = pd.read_csv(
            io.StringIO(csv_data),
            skiprows=skiprows,
            header=0,
            sep="\t",
            engine="python",
            on_bad_lines="skip",
        )
    return df


with DAG(
    dag_id="dotacao_execucao_outras_fontes_mcid_ingest_dag",
    default_args=default_args,
    description="Processa e ingere dados de execução de outras fontes de MCID",
    schedule_interval=get_dynamic_schedule("dotacao_execucao_outras_fontes_mcid"),
    catchup=False,
    start_date=datetime(2026, 3, 27),
    tags=["email", "mcid", "tesouro", "dotacao", "execucao"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        cliente_email.format_csv = _patched_format_csv

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
                "CSV processado com sucesso. Registros encontrados: %s", len(csv_data)
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

            df = pd.read_csv(io.StringIO(csv_data))
            data = df.to_dict(orient="records")

            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)


            db.insert_data(
                data,
                "dotacao_execucao_outras_fontes_mcid",
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
