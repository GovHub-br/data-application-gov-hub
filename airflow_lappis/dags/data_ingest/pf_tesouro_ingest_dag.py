from typing import Dict, Any, Optional, List, cast
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
import pandas as pd
import io
from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

# Configurações básicas da DAG
default_args = {
    "owner": "Davi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Mapeamento das colunas para as programações financeiras
COLUMN_MAPPING = {
    0: "emissao_mes",
    1: "emissao_dia",
    2: "ug_emitente",
    3: "ug_emitente_descricao",
    4: "ug_favorecido",
    5: "ug_favorecido_descricao",
    6: "pf_evento",
    7: "pf_evento_descricao",
    8: "pf",
    9: "pf_inscricao",
    10: "pf_acao",
    11: "pf_acao_descricao",
    12: "pf_fonte_recursos",
    13: "pf_fonte_recursos_descricao",
    14: "pf_vinculacao_pagamento",
    15: "pf_vinculacao_pagamento_descricao",
    16: "pf_categoria_gasto",
    17: "pf_recurso",
    18: "pf_recurso_descricao",
    19: "doc_observacao",
    20: "pf_valor_linha",
}

# Assuntos dos emails a serem processados
EMAIL_SUBJECT_ENVIADAS = "programacoes_financeiras_enviadas_devolvidas_a_partir_de_2024"
EMAIL_SUBJECT_RECEBIDAS = "programacoes_financeiras_recebidas_a_partir_de_2024"
SKIPROWS = 3

# Configurações da DAG
with DAG(
    dag_id="email_programacoes_financeiras_ingest",
    default_args=default_args,
    description="Processa anexos das PFs vindo de dois emails, formata e insere no db",
    schedule_interval="0 13 * * 1-6",
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "pfs", "tesouro"],
) as dag:

    def process_email_data_enviadas(**context: Dict[str, Any]) -> Optional[List[Dict]]:
        """
        Função para processar os emails com programações financeiras enviadas.
        """
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        try:
            logging.info(
                "Iniciando o processamento dos emails de programações enviadas/devolvidas"
            )
            csv_data = cast(
                Optional[List[Dict[Any, Any]]],
                fetch_and_process_email(
                    IMAP_SERVER,
                    EMAIL,
                    PASSWORD,
                    SENDER_EMAIL,
                    EMAIL_SUBJECT_ENVIADAS,
                    COLUMN_MAPPING,
                    skiprows=SKIPROWS,
                ),
            )
            if not csv_data:
                logging.warning(
                    "Nenhum e-mail encontrado com o assunto de programações enviadas"
                )
                return []

            logging.info(
                "CSV de PFs enviadas processado com sucesso. Dados encontrados: %s",
                len(csv_data),
            )
            return csv_data
        except Exception as e:
            logging.error(
                "Erro no processamento dos emails de programações enviadas: %s",
                str(e),
            )
            raise

    def process_email_data_recebidas(**context: Dict[str, Any]) -> Optional[List[Dict]]:
        """
        Função para processar os emails com programações financeiras recebidas.
        """
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        try:
            logging.info(
                "Iniciando o processamento dos emails de programações recebidas..."
            )
            csv_data = cast(
                Optional[List[Dict[Any, Any]]],
                fetch_and_process_email(
                    IMAP_SERVER,
                    EMAIL,
                    PASSWORD,
                    SENDER_EMAIL,
                    EMAIL_SUBJECT_RECEBIDAS,
                    column_mapping=None,
                    skiprows=SKIPROWS,
                ),
            )
            if not csv_data:
                logging.warning(
                    "Nenhum e-mail encontrado com o assunto de programações recebidas."
                )
                return []

            logging.info(
                "CSV de PFs recebidas processado com sucesso. Dados encontrados: %s",
                len(csv_data),
            )
            return csv_data
        except Exception as e:
            logging.error(
                "Erro no processamento dos emails de programações recebidas: %s", str(e)
            )
            raise

    def combine_data(**context: Dict[str, Any]) -> List[Dict]:
        """
        Função para combinar os dados dos dois emails.
        """
        try:
            task_instance: Any = context["ti"]
            enviadas_data = (
                task_instance.xcom_pull(task_ids="process_emails_enviadas") or []
            )
            recebidas_data = (
                task_instance.xcom_pull(task_ids="process_emails_recebidas") or []
            )

            combined_data = enviadas_data + recebidas_data

            logging.info(f"Dados combinados: {len(combined_data)} registros no total.")
            return combined_data
        except Exception as e:
            logging.error(f"Erro ao combinar os dados: {str(e)}")
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        """
        Função para inserir os dados no banco de dados.
        Os dados combinados são recuperados do XCom.
        """
        try:
            task_instance: Any = context["ti"]
            combined_data = task_instance.xcom_pull(task_ids="combine_data")

            if not combined_data:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            df = pd.read_csv(io.StringIO(combined_data))
            data = df.to_dict(orient="records")

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            db.insert_data(data, "pf_tesouro", schema="siafi")
            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
            raise

    def clean_duplicates(**context: Dict[str, Any]) -> None:
        """
        Task para remover duplicados da tabela 'siafi.pf_tesouro'.
        """
        try:
            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)
            db.remove_duplicates("pf_tesouro", COLUMN_MAPPING, schema="siafi")

        except Exception as e:
            logging.error(f"Erro ao executar a limpeza de duplicados: {str(e)}")
            raise

    # Tarefa 1: Processar os e-mails de programações enviadas/devolvidas
    process_emails_enviadas_task = PythonOperator(
        task_id="process_emails_enviadas",
        python_callable=process_email_data_enviadas,
        provide_context=True,
    )

    # Tarefa 2: Processar os e-mails de programações recebidas
    process_emails_recebidas_task = PythonOperator(
        task_id="process_emails_recebidas",
        python_callable=process_email_data_recebidas,
        provide_context=True,
    )

    # Tarefa 3: Combinar os dados dos dois emails
    combine_data_task = PythonOperator(
        task_id="combine_data",
        python_callable=combine_data,
        provide_context=True,
    )

    # Tarefa 4: Inserir os dados no db
    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_db,
        provide_context=True,
    )

    # Tarefa 5: Limpar duplicados no banco de dados
    clean_duplicates_task = PythonOperator(
        task_id="clean_duplicates",
        python_callable=clean_duplicates,
        provide_context=True,
    )

    # Fluxo da DAG
    (
        [process_emails_enviadas_task, process_emails_recebidas_task]
        >> combine_data_task
        >> insert_to_db_task
        >> clean_duplicates_task
    )
