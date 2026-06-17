from typing import Dict, Any
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

# Configurações básicas da DAG
default_args = {
    "owner": "Tiago",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Mapeamento das colunas para as programações financeiras (recebidas)
COLUMN_MAPPING = {
    0: "programa_governo",
    1: "programa_governo_descricao",
    2: "plano_orcamentario",
    3: "plano_orcamentario_descricao_1",
    4: "plano_orcamentario_descricao_2",
    5: "plano_orcamentario_descricao_3",
    6: "plano_orcamentario_descricao_4",
    7: "plano_orcamentario_descricao_5",
    8: "plano_orcamentario_descricao_6",
    9: "acao_governo",
    10: "acao_governo_descricao",
    11: "ptres",
    12: "natureza_despesa",
    13: "natureza_despesa_descricao",
    14: "dotacao_inicial",
    15: "dotacao_suplementar",
    16: "dotacao_atualizada",
}

# Assunto do email a ser processado
EMAIL_SUBJECT = "programacao_acao_por_ptres_mir"
SKIPROWS = 5

# Configurações da DAG
with DAG(
    dag_id="email_programacao_acao_por_PTRES_ingest",
    default_args=default_args,
    description="Processa anexo consolidado de programações de ação por PTRES por email e insere no db",
    schedule_interval=get_dynamic_schedule("programacao_acao_ptres_ingest_dag"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "ptres", "tesouro", "MIR"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> str:
        """
        Função para processar o email com programações financeiras.
        """
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        try:
            logging.info("Iniciando o processamento do email de programações financeiras")
            csv_data = fetch_and_process_email(
                IMAP_SERVER,
                EMAIL,
                PASSWORD,
                SENDER_EMAIL,
                EMAIL_SUBJECT,
                column_mapping=COLUMN_MAPPING,
                skiprows=SKIPROWS,
            )
            if not csv_data:
                logging.warning("Nenhum e-mail encontrado com o assunto configurado")
                return ""

            logging.info("CSV de programações de ação por PTRES processado com sucesso.")
            return csv_data
        except Exception as e:
            logging.error(
                "Erro no processamento do email de programações de ação por PTRES: %s",
                str(e),
            )
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        """
        Função para inserir os dados no banco de dados.
        Os dados processados são recuperados do XCom.
        """
        try:
            task_instance: Any = context["ti"]
            processed_data = task_instance.xcom_pull(task_ids="process_email")

            if not processed_data:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            df = pd.read_csv(io.StringIO(processed_data))
            data = df.to_dict(orient="records")

            # Adicionar dt_ingest a cada registro
            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn("postgres_mir")
            db = ClientPostgresDB(postgres_conn_str)

            db.insert_data(data, "programacao_acao_ptres", schema="siafi")
            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
            raise

    def clean_duplicates(**context: Dict[str, Any]) -> None:
        """
        Task para remover duplicados da tabela 'siafi.programacao_acao_ptres'.
        """
        try:
            postgres_conn_str = get_postgres_conn("postgres_mir")
            db = ClientPostgresDB(postgres_conn_str)
            db.remove_duplicates("programacao_acao_ptres", COLUMN_MAPPING, schema="siafi")

        except Exception as e:
            logging.error(f"Erro ao executar a limpeza de duplicados: {str(e)}")
            raise

    # Tarefa 1: Processar o email com os dados consolidados
    process_email_task = PythonOperator(
        task_id="process_email",
        python_callable=process_email_data,
        provide_context=True,
    )

    # Tarefa 2: Inserir os dados no db
    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_db,
        provide_context=True,
    )

    # Tarefa 3: Limpar duplicados no banco de dados
    clean_duplicates_task = PythonOperator(
        task_id="clean_duplicates",
        python_callable=clean_duplicates,
        provide_context=True,
    )

    # Fluxo da DAG
    process_email_task >> insert_to_db_task >> clean_duplicates_task
