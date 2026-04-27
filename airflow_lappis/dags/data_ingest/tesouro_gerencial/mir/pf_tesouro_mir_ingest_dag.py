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
    14: "doc_observacao",
    15: "pf_valor_linha",
}

# Assunto do email a ser processado
EMAIL_SUBJECT = "programacoes_financeiras"
SKIPROWS = 7

# Configurações da DAG
with DAG(
    dag_id="pf_tesouro_ingest", # pf programações financeiras
    default_args=default_args,
    description="Processa anexo consolidado de PFs por email e insere no db",
    schedule_interval=get_dynamic_schedule("pf_tesouro_mir_ingest_dag"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "pfs", "tesouro", "MIR"],
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

            logging.info("CSV de PFs processado com sucesso.")
            return csv_data
        except Exception as e:
            logging.error(
                "Erro no processamento do email de programações financeiras: %s",
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

            db.insert_data(data, "pf_tesouro", schema="tesouro_gerencial")
            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
            raise

    def clean_duplicates(**context: Dict[str, Any]) -> None:
        """
        Task para remover duplicados da tabela 'siafi.pf_tesouro'.
        """
        try:
            postgres_conn_str = get_postgres_conn("postgres_mir")
            db = ClientPostgresDB(postgres_conn_str)
            db.remove_duplicates("pf_tesouro", COLUMN_MAPPING, schema="tesouro_gerencial")

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
