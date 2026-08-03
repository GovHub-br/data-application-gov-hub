from datetime import datetime, timedelta
from typing import Any, Dict, Optional
import io
import json
import logging

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


default_args = {
    "owner": "Mateus",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Layout do relatório "Notas de crédito enviadas/devolvidas 2026" do Tesouro
# Gerencial. O anexo CSV é UTF-16 e separado por tabulação.
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

EMAIL_SUBJECT = "notas_credito_enviadas_devolvidas_ipea"
SKIPROWS = 3
RAW_TABLE = "nc_tesouro_pos_2026"
UNIQUE_KEY = [
    "nc",
    "emissao_dia",
    "emissao_mes",
    "emissao_ano",
    "ptres",
    "ugr_codigo",
    "gnd_codigo",
    "pi_codigo",
    "ro",
    "valor_celula",
    "item_total",
    "dc",
]


with DAG(
    dag_id="email_notas_credito_ingest_ipea_pos_2026",
    default_args=default_args,
    description="Ingere NCs do relatório pós-2026 do Tesouro Gerencial para o IPEA",
    schedule_interval=get_dynamic_schedule("nc_tesouro_ingest_2026_dag"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["IPEA", "SIAFI", "notas_credito", "pos_2026"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[str]:
        del context
        creds = json.loads(Variable.get("email_credentials"))

        try:
            logging.info("Iniciando coleta de NCs pós-2026: %s", EMAIL_SUBJECT)
            csv_data = fetch_and_process_email(
                creds["imap_server"],
                creds["email"],
                creds["password"],
                creds["sender_email"],
                EMAIL_SUBJECT,
                COLUMN_MAPPING_NC,
                skiprows=SKIPROWS,
                delimiter="\t",
            )
            if not csv_data:
                logging.warning("Nenhum CSV de NC pós-2026 encontrado.")
                return None

            return csv_data
        except Exception as exc:
            logging.error("Erro ao processar e-mail de NC pós-2026: %s", exc)
            raise

    def insert_data_to_db(**context: Any) -> None:
        try:
            csv_data = context["ti"].xcom_pull(task_ids="process_emails")
            if not csv_data:
                logging.warning("Nenhum dado de NC pós-2026 para inserir.")
                return

            df = pd.read_csv(io.StringIO(csv_data), dtype=str, keep_default_na=False)
            expected_columns = list(COLUMN_MAPPING_NC.values())
            if list(df.columns) != expected_columns:
                raise ValueError(
                    "Layout inesperado de NC pós-2026. "
                    f"Esperadas {len(expected_columns)} colunas; recebidas {len(df.columns)}."
                )

            df["dt_ingest"] = datetime.now().isoformat()
            data = df.to_dict(orient="records")

            db = ClientPostgresDB(get_postgres_conn())
            db.insert_data(
                data,
                RAW_TABLE,
                conflict_fields=UNIQUE_KEY,
                schema="siafi",
            )
            logging.info("Carga de NC pós-2026 finalizada: %s registros.", len(data))
        except Exception as exc:
            logging.error("Erro na inserção de NC pós-2026: %s", exc)
            raise

    process_emails_task = PythonOperator(
        task_id="process_emails",
        python_callable=process_email_data,
        show_return_value_in_logs=False,
    )
    insert_to_db_task = PythonOperator(
        task_id="insert_to_db", python_callable=insert_data_to_db
    )

    process_emails_task >> insert_to_db_task
