from typing import Dict, Any, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
from schedule_loader import get_dynamic_schedule
from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
import pandas as pd
import io

# Configurações básicas da DAG
default_args = {
    "owner": "Tiago",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COLUMN_MAPPING = {
    0: "programa_governo",
    1: "programa_governo_descricao",
    2: "acao_governo",
    3: "acao_governo_descricao",
    4: "emissao_mes",
    5: "emissao_dia",
    6: "ne_ccor",
    7: "ne_num_processo",
    8: "ne_info_complementar",
    9: "ne_ccor_descricao",
    10: "doc_observacao",
    11: "natureza_despesa",
    12: "natureza_despesa_descricao",
    13: "ne_ccor_favorecido",
    14: "ne_ccor_favorecido_descricao",
    15: "ne_ccor_ano_emissao",
    16: "ptres",
    17: "fonte_recursos_detalhada",
    18: "fonte_recursos_detalhada_descricao",
    19: "despesas_empenhadas",
    20: "despesas_liquidadas",
    21: "despesas_pagas",
    22: "restos_a_pagar_inscritos",
    23: "restos_a_pagar_pagos",
}

EMAIL_SUBJECT = "notas_de_empenho_ano_atual"
SKIPROWS = 8

# Configurações da DAG
with DAG(
    dag_id="email_tesouro_parlamentares_ingest",
    default_args=default_args,
    description="Processa anexos dos empenhos vindo do email, formata e insere no db",
    schedule_interval=get_dynamic_schedule("empenhos_tesouro_parlamentares_ingest_dag"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["MIR", "email", "empenhos", "tesouro", "parlamentares"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        try:
            logging.info("Iniciando o processamento dos emails...")
            csv_path = fetch_and_process_email(
                IMAP_SERVER,
                EMAIL,
                PASSWORD,
                SENDER_EMAIL,
                EMAIL_SUBJECT,
                COLUMN_MAPPING,
                skiprows=SKIPROWS,
                output_path=f"/tmp/{context['dag'].dag_id}_{context['ts_nodash']}.csv",
            )
            if not csv_path:
                logging.warning(
                    "Nenhum CSV valido foi extraido dos e-mails encontrados "
                    "para o assunto esperado."
                )
                return None

            logging.info(
                "CSV processado com sucesso e gravado em: %s",
                csv_path,
            )
            return csv_path
        except Exception as e:
            logging.error("Erro no processamento dos emails: %s", str(e))
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        """
        Função para inserir os dados no banco de dados.
        Os dados do CSV são recuperados do XCom.
        """
        try:
            ti = context["ti"]
            csv_path = ti.xcom_pull(task_ids="process_emails")

            if not csv_path:
                logging.warning("Caminho do CSV não encontrado no XCom")
                return

            df = pd.read_csv(csv_path)

            df = df[df["ne_ccor_ano_emissao"].astype(str).str.startswith("20")]
            data = df.to_dict(orient="records")

            # Adicionar dt_ingest a cada registro
            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn("postgres_mir")
            db = ClientPostgresDB(postgres_conn_str)

            unique_key = [
                "ne_ccor",
                "natureza_despesa",
                "doc_observacao",
                "ne_ccor_ano_emissao",
                "emissao_dia",
                "emissao_mes",
                "despesas_empenhadas",
                "despesas_liquidadas",
                "despesas_pagas",
            ]

            db.insert_data(
                data,
                "empenhos_tesouro_parlamentares",
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
