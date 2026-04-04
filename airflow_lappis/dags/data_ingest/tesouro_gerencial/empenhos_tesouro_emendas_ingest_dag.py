from typing import Dict, Any, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.param import Param
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
    0: "emissao_mes",
    1: "emissao_dia",
    2: "programa_governo",
    3: "programa_governo_descricao",
    4: "acao_governo",
    5: "acao_governo_descricao",
    6: "autor_emendas_orcamento",
    7: "autor_emendas_orcamento_descricao",
    8: "uf_pt",
    9: "uf_pt_descricao",
    10: "municipio_pt",
    11: "ne_ccor",
    12: "ne_num_processo",
    13: "ne_info_complementar",
    14: "ne_ccor_descricao",
    15: "doc_observacao",
    16: "grupo_despesa",
    17: "grupo_despesa_descricao",
    18: "natureza_despesa",
    19: "natureza_despesa_descricao",
    20: "modalidade_aplicacao",
    21: "modalidade_aplicacao_descricao",
    22: "ne_ccor_favorecido",
    23: "ne_ccor_favorecido_descricao",
    24: "ne_ccor_ano_emissao",
    25: "ptres",
    26: "item_informacao",
    27: "item_informacao_descricao",
    28: "despesas_empenhadas",
    29: "despesas_liquidadas",
    30: "despesas_pagas",
    31: "restos_a_pagar_inscritos",
    32: "restos_a_pagar_pagos",
}

EMAIL_SUBJECT = "notas_de_empenhos_emendas_parlamentares"
SKIPROWS = 12

# Configurações da DAG
with DAG(
    dag_id="email_tesouro_emendas_ingest",
    default_args=default_args,
    description="Processa anexos dos empenhos vindo do email, formata e insere no db",
    schedule_interval=get_dynamic_schedule("empenhos_tesouro_emendas_ingest_dag"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    params={
        "data_referencia": Param(
            default=None,
            type=["string", "null"],
            title="Data de Referencia",
            description=(
                "Data para filtrar os e-mails recebidos (formato YYYY-MM-DD). "
                "Se nao informado, usa o dia atual."
            ),
        )
    },
    tags=["MIR", "email", "empenhos", "tesouro", "emendas"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]
        params = context.get("params", {})
        data_referencia = params.get("data_referencia")

        target_date = None
        if data_referencia:
            try:
                target_date = datetime.strptime(data_referencia, "%Y-%m-%d").date()
            except ValueError as exc:
                raise ValueError(
                    "Parametro 'data_referencia' invalido. Use o formato YYYY-MM-DD."
                ) from exc

        try:
            logging.info(
                "Iniciando o processamento dos emails para a data: %s",
                target_date.isoformat() if target_date else "dia atual",
            )
            csv_data = fetch_and_process_email(
                IMAP_SERVER,
                EMAIL,
                PASSWORD,
                SENDER_EMAIL,
                EMAIL_SUBJECT,
                COLUMN_MAPPING,
                skiprows=SKIPROWS,
                target_date=target_date,
            )
            if not csv_data:
                logging.warning(
                    "Nenhum CSV valido foi extraido dos e-mails encontrados "
                    "para o assunto esperado."
                )
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
                "empenhos_tesouro_emendas_parlamentares",
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
