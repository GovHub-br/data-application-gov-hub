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
from functools import partial

default_args = {
    "owner": "Mateus",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

pd.read_csv = partial(pd.read_csv, sep='\t', on_bad_lines='skip')

COLUMN_MAPPING_NC = {
    0: "emissao_dia",
    1: "nc",
    2: "emitente_codigo",
    3: "emitente_nome",
    4: "ptres",
    5: "fonte_codigo",
    6: "fonte_nome",
    7: "gnd_codigo",
    8: "gnd_nome",
    9: "pi_codigo",
    10: "pi_nome",
    11: "descricao",
    12: "ugr_codigo",
    13: "ugr_nome",
    14: "tipo_nc",
    15: "nc_item_detalhamento",
    16: "favorecido_codigo",
    17: "favorecido_nome",
    18: "ro",
    19: "nc_transferencia",
    20: "dc",
    21: "item_total",
    22: "total_lista",
    23: "valor_celula",
    24: "esfera_orcamentaria_codigo",
    25: "esfera_orcamentaria_nome",
    26: "emissao_ano",
    27: "emissao_mes",
}

EMAIL_SUBJECT = "notas_credito_mir_apos_2026"
SKIPROWS = 3

with DAG(
    dag_id="email_notas_credito_ingest_mir_pos_2026",
    default_args=default_args,
    schedule_interval=get_dynamic_schedule("email_notas_credito_ingest_mir_post_2026"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["MIR", "email", "notas_credito"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        try:
            logging.info("Iniciando coleta de emails para o assunto: %s", EMAIL_SUBJECT)
            csv_data = fetch_and_process_email(
                creds["imap_server"],
                creds["email"],
                creds["password"],
                creds["sender_email"],
                EMAIL_SUBJECT,
                COLUMN_MAPPING_NC,
                skiprows=SKIPROWS,
            )
            
            if not csv_data:
                logging.warning("Nenhum CSV extraído.")
                return None

            return csv_data
        except Exception as e:
            logging.error("Erro no processamento: %s", str(e))
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        try:
            ti = context["ti"]
            csv_data = ti.xcom_pull(task_ids="process_emails")

            if not csv_data:
                return

            df = pd.read_csv(io.StringIO(csv_data), sep=',')
            data = df.to_dict(orient="records")
            
            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn("postgres_mir")
            db = ClientPostgresDB(postgres_conn_str)

            unique_key = [
                "nc",
                "emissao_dia",
                "emissao_mes",
                "emissao_ano",
                "ptres",
                "ugr_codigo",
                "valor_celula",
                "dc",
            ]

            db.insert_data(
                data,
                "nc_tesouro_pos__2026",
                conflict_fields=unique_key,
                primary_key=unique_key,
                schema="siafi",
            )
            logging.info("Carga finalizada com sucesso.")
        except Exception as e:
            logging.error("Erro na inserção: %s", str(e))
            raise

    process_emails_task = PythonOperator(
        task_id="process_emails",
        python_callable=process_email_data,
    )

    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_db,
    )

    process_emails_task >> insert_to_db_task