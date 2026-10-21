import io
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import cliente_email  # importar o módulo, não só a função
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
    0: "acao_governo_codigo",
    1: "acao_governo_nome",
    2: "programa_governo_codigo",
    3: "programa_governo_nome",
    4: "ne_ccor",
    5: "ne_ccor_favorecido_codigo",
    6: "ne_ccor_favorecido_nome",
    7: "favorecido_cep",
    8: "favorecido_municipio_codigo",
    9: "favorecido_municipio_nome",
    10: "favorecido_regiao",
    11: "favorecido_ug_uf_codigo",
    12: "favorecido_ug_uf_nome",
    13: "fonte_recursos_detalhada",
    14: "fonte_recursos_detalhada_descricao",
    15: "pt",
    16: "ptres",
    17: "plano_orcamentario_ug_executora_codigo",
    18: "plano_orcamentario_cod1",
    19: "plano_orcamentario_cod2",
    20: "plano_orcamentario_programa",
    21: "plano_orcamentario_acao_orcamentaria",
    22: "plano_orcamentario_medida",
    23: "plano_orcamentario_descricao",
    24: "ug_executora_codigo",
    25: "ug_executora_nome",
    26: "ug_responsavel_codigo",
    27: "ug_responsavel_nome",
    28: "pl_codigo",
    29: "pl_nome",
    30: "natureza_despesa_codigo",
    31: "natureza_despesa_nome",
    32: "dotacao_inicial",
    33: "dotacao_atualizada",
    34: "despesas_empenhadas",
    35: "despesas_empenhadas_a_liquidar",
    36: "despesas_liquidadas_a_pagar",
    37: "despesas_pagas",
}

EMAIL_SUBJECT = "orcamento_mcid_por_acao"
SKIPROWS = 10


# A formatação do CSV estava como utf-16.
# Função criada para consumo sem erro de formatação
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
    dag_id="orcamento_mcid_por_acao_ingest_dag",
    default_args=default_args,
    description="Processa e ingere dados de orcamento por acao do MCID do Tesouro",
    schedule_interval=get_dynamic_schedule("orcamento_mcid_por_acao_ingest_dag"),
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=["email", "orcamento", "tesouro", "mcid"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = creds["sender_email"]

        # Monkey-patch: substitui format_csv do cliente_email pela versão corrigida
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
                "orcamento_mcid_por_acao",
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
