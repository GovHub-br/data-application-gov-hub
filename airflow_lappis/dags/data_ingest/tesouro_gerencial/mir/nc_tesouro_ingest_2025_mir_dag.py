from typing import Dict, Any, cast
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

default_args = {
    "owner": "Mateus",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COLUMN_MAPPING = {
    0: "programa_governo",
    1: "programa_governo_descricao",
    2: "acao_governo",
    3: "acao_governo_descricao",
    4: "nc",
    5: "nc_transferencia", 
    6: "nc_fonte_recursos",
    7: "nc_fonte_recursos_descricao",
    8: "ptres",
    9: "nc_evento",
    10: "nc_evento_descricao",
    11: "nc_ug_responsavel",
    12: "nc_ug_responsavel_descricao",
    13: "nc_natureza_despesa",
    14: "nc_natureza_despesa_descricao",
    15: "nc_plano_interno",
    16: "nc_plano_interno_descricao1",
    17: "nc_plano_interno_descricao2",
    18: "favorecido_doc",
    19: "favorecido_doc_descricao",
    20: "favorecido_municipio",
    21: "favorecido_municipio_descricao",
    22: "nc_valor_linha",
    23: "movimento_liquido_moeda_origem",
}

# Configurações dos emails
EMAIL_CONFIGS = {
    "enviadas": {
        "subject": "notas_credito_mir_ate_2025",
        "column_mapping": COLUMN_MAPPING,
        "skiprows": 6,
    },
    "recebidas": {
        "subject": "notas_credito_recebidas_ate_2025",
        "column_mapping": None,
        "skiprows": 6,
    },
}
expected_columns = list(COLUMN_MAPPING.values())
with DAG(
    dag_id="email_notas_credito_ingest_mir_ate_2025",
    default_args=default_args,
    schedule_interval=get_dynamic_schedule("email_notas_credito_ingest_mir_ate_2025"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["MIR", "SIAFI", "notas_credito"],
) as dag:

    def process_email_data(email_type: str, **context: Dict[str, Any]) -> pd.DataFrame:
        """
        Função genérica para processar emails de notas de crédito.
        """
        config = EMAIL_CONFIGS[email_type]
        creds_data = json.loads(Variable.get("email_credentials"))
        creds = cast(Dict[str, str], creds_data)
        config = cast(Dict[str, Any], config)

        try:
            logging.info(f"Iniciando o processamento das NCs {email_type}")
            csv_data = fetch_and_process_email(
                creds["imap_server"],
                creds["email"],
                creds["password"],
                creds["sender_email"],
                config["subject"],
                config["column_mapping"],
                skiprows=config["skiprows"],
            )

            if not csv_data:
                logging.warning(f"Nenhum e-mail encontrado para NCs {email_type}")
                return pd.DataFrame()

            df = pd.read_csv(io.StringIO(csv_data))

            # Se não tem mapeamento de colunas (recebidas), aplicar o mapeamento padrão
            if config["column_mapping"] is None and not df.empty:
                expected_columns = list(COLUMN_MAPPING.values())
                if len(df.columns) == len(expected_columns):
                    df.columns = pd.Index(expected_columns)
                else:
                    logging.warning(
                        f"N coluna incompatível:{len(expected_columns)},{len(df.columns)}"
                    )

            logging.info(
                f"CSV de NCs {email_type} processado com sucesso: {len(df)} registros"
            )
            return df

        except Exception as e:
            logging.error(
                f"Erro no processamento dos emails de NCs {email_type}: {str(e)}"
            )
            raise

    def process_email_data_enviadas(**context: Dict[str, Any]) -> pd.DataFrame:
        """Wrapper para processar emails enviadas."""
        return process_email_data("enviadas", **context)

    def process_email_data_recebidas(**context: Dict[str, Any]) -> pd.DataFrame:
        """Wrapper para processar emails recebidas."""
        return process_email_data("recebidas", **context)

    def combine_data(**context: Dict[str, Any]) -> pd.DataFrame:
        """
        Função para combinar os dados dos dois emails.
        """
        try:
            task_instance: Any = context["ti"]
            df_enviadas = cast(
                pd.DataFrame, task_instance.xcom_pull(task_ids="process_emails_enviadas")
            )
            df_recebidas = cast(
                pd.DataFrame, task_instance.xcom_pull(task_ids="process_emails_recebidas")
            )

            dfs = [
                df
                for df in [df_enviadas, df_recebidas]
                if df is not None and not df.empty
            ]

            if not dfs:
                logging.warning("Nenhum dado foi encontrado para combinar.")
                return pd.DataFrame()

            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df["dt_ingest"] = datetime.now().isoformat()

            logging.info(f"Dados combinados: {len(combined_df)} registros no total.")
            return combined_df

        except Exception as e:
            logging.error(f"Erro ao combinar os dados: {str(e)}")
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        """
        Função para inserir os dados no banco de dados.
        """
        try:
            task_instance: Any = context["ti"]
            combined_df = task_instance.xcom_pull(task_ids="combine_data")

            if combined_df is None or combined_df.empty:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            data = combined_df.to_dict(orient="records")

            postgres_conn_str = get_postgres_conn('postgres_mir')
            db = ClientPostgresDB(postgres_conn_str)

            db.insert_data(data, "nc_tesouro_pre_2026", schema="siafi")
            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
            raise

    def clean_duplicates(**context: Dict[str, Any]) -> None:
        """
        Task para remover duplicados da tabela 'siafi.pf_tesouro'.
        """
        try:
            postgres_conn_str = get_postgres_conn('postgres_mir')
            db = ClientPostgresDB(postgres_conn_str)
            db.remove_duplicates("nc_tesouro_pre_2026", COLUMN_MAPPING, schema="siafi")

        except Exception as e:
            logging.error(f"Erro ao executar a limpeza de duplicados: {str(e)}")
            raise

    process_emails_enviadas_task = PythonOperator(
        task_id="process_emails_enviadas",
        python_callable=process_email_data_enviadas,
        provide_context=True,
    )

    process_emails_recebidas_task = PythonOperator(
        task_id="process_emails_recebidas",
        python_callable=process_email_data_recebidas,
        provide_context=True,
    )

    combine_data_task = PythonOperator(
        task_id="combine_data",
        python_callable=combine_data,
        provide_context=True,
    )

    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_db,
        provide_context=True,
    )

    clean_duplicates_task = PythonOperator(
        task_id="clean_duplicates",
        python_callable=clean_duplicates,
        provide_context=True,
    )

    (
        [process_emails_enviadas_task, process_emails_recebidas_task]
        >> combine_data_task
        >> insert_to_db_task
        >> clean_duplicates_task
    )
