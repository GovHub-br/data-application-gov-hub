import io
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule


def build_email_ingest_dag(
    dag_id: str,
    email_subject: str,
    column_mapping: Dict[int, str],
    skiprows: int,
    table_name: str,
    schedule_key: str,
    schema: str = "siafi",
    postgres_conn_key: str = "postgres_default",
    owner: str = "Davi",
    start_date: datetime = datetime(2023, 12, 1),
    tags: Optional[list] = None,
    description: str = "",
) -> DAG:
    """
    Gera DAGs padrao de ingestao via email do Tesouro Gerencial.
    Fluxo: process_emails -> insert_to_db -> clean_duplicates.
    """
    default_args = {
        "owner": owner,
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=get_dynamic_schedule(schedule_key),
        start_date=start_date,
        catchup=False,
        tags=tags or [],
    ) as dag:

        def _process_emails(**context: Dict[str, Any]) -> Optional[str]:
            creds = json.loads(Variable.get("email_credentials"))
            try:
                logging.info("Iniciando o processamento dos emails...")
                csv_data = fetch_and_process_email(
                    creds["imap_server"],
                    creds["email"],
                    creds["password"],
                    creds["sender_email"],
                    email_subject,
                    column_mapping,
                    skiprows=skiprows,
                )
                if not csv_data:
                    logging.warning("Nenhum e-mail encontrado com o assunto esperado.")
                    return None
                logging.info(
                    "CSV processado com sucesso. Dados encontrados: %s", len(csv_data)
                )
                return csv_data
            except Exception:
                logging.exception("Erro no processamento dos emails.")
                raise

        def _insert_to_db(**context: Dict[str, Any]) -> None:
            csv_data = context["ti"].xcom_pull(task_ids="process_emails")
            if not csv_data:
                logging.warning("Nenhum dado para inserir no banco.")
                return
            df = pd.read_csv(io.StringIO(csv_data))
            data = df.to_dict(orient="records")
            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()
            db = ClientPostgresDB(get_postgres_conn(postgres_conn_key))
            db.insert_data(data, table_name, schema=schema)
            logging.info("Dados inseridos com sucesso no banco de dados.")

        def _clean_duplicates(**context: Dict[str, Any]) -> None:
            try:
                db = ClientPostgresDB(get_postgres_conn(postgres_conn_key))
                db.remove_duplicates(table_name, column_mapping, schema=schema)
            except Exception:
                logging.exception("Erro ao executar a limpeza de duplicados.")
                raise

        process_task = PythonOperator(
            task_id="process_emails",
            python_callable=_process_emails,
            provide_context=True,
        )
        insert_task = PythonOperator(
            task_id="insert_to_db",
            python_callable=_insert_to_db,
            provide_context=True,
        )
        clean_task = PythonOperator(
            task_id="clean_duplicates",
            python_callable=_clean_duplicates,
            provide_context=True,
        )

        process_task >> insert_task >> clean_task

    return dag
