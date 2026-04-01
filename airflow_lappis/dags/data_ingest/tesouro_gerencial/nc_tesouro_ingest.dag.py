from typing import Dict, Any, cast
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
import pandas as pd
from schedule_loader import get_dynamic_schedule
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

# Mapeamento das colunas para as notas de crédito
COLUMN_MAPPING = {
    0: "emissao_mes",
    1: "emissao_dia",
    2: "nc",
    3: "nc_transferencia",
    4: "nc_fonte_recursos",
    5: "nc_fonte_recursos_descricao",
    6: "ptres",
    7: "nc_evento",
    8: "nc_evento_descricao",
    9: "ug_responsavel",
    10: "ug_responsavel_descricao",
    11: "natureza_despesa",
    12: "natureza_despesa_detalhada",
    13: "plano_interno",
    14: "plano_detalhado_descricao1",
    15: "plano_detalhado_descricao2",
    16: "favorecido_doc",
    17: "favorecido_doc_descricao",
    18: "nc_valor_linha",
    19: "movimento_liquido",
}

# Configurações dos emails
EMAIL_CONFIGS = {
    "enviadas": {
        "subject": "notas_credito_enviadas_devolvidas_a_partir_de_2024",
        "column_mapping": COLUMN_MAPPING,
        "skiprows": 10,
    },
    "recebidas": {
        "subject": "notas_credito_recebidas_a_partir_de_2024",
        "column_mapping": None,
        "skiprows": 6,
    },
}

# Configurações da DAG
with DAG(
    dag_id="email_notas_credito_ingest",
    default_args=default_args,
    description="Processa anexos das NCs vindo de dois emails, formata e insere no db",
    schedule_interval=get_dynamic_schedule("nc_tesouro_ingest_dag"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "ncs", "tesouro"],
) as dag:

    def process_email_data(email_type: str, **context: Dict[str, Any]) -> str:
        """
        Função genérica para processar emails de notas de crédito.
        Retorna sempre o caminho do arquivo CSV processado.
        """
        config = EMAIL_CONFIGS[email_type]
        creds_data = json.loads(Variable.get("email_credentials"))
        creds = cast(Dict[str, str], creds_data)
        config = cast(Dict[str, Any], config)

        try:
            logging.info(f"Iniciando o processamento das NCs {email_type}")
            csv_path = fetch_and_process_email(
                creds["imap_server"],
                creds["email"],
                creds["password"],
                creds["sender_email"],
                config["subject"],
                config["column_mapping"],
                skiprows=config["skiprows"],
                output_path=f"/tmp/{context['dag'].dag_id}_{context['ts_nodash']}.csv",
            )

            if not csv_path:
                logging.warning(f"Nenhum e-mail encontrado para NCs {email_type}")
                return ""

            df = pd.read_csv(csv_path)

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
            # sobrescrever o arquivo com o schema final
            df.to_csv(csv_path, index=False)
            return csv_path

        except Exception as e:
            logging.error(
                f"Erro no processamento dos emails de NCs {email_type}: {str(e)}"
            )
            raise

    def process_email_data_enviadas(**context: Dict[str, Any]) -> str:
        """Wrapper para processar emails enviadas, retornando caminho do CSV."""
        return process_email_data("enviadas", **context)

    def process_email_data_recebidas(**context: Dict[str, Any]) -> str:
        """Wrapper para processar emails recebidas, retornando caminho do CSV."""
        return process_email_data("recebidas", **context)

    def combine_data(**context: Dict[str, Any]) -> str:
        """Combina os dados dos dois emails e retorna caminho do CSV."""
        try:
            task_instance: Any = context["ti"]
            path_enviadas = cast(
                str, task_instance.xcom_pull(task_ids="process_emails_enviadas") or ""
            )
            path_recebidas = cast(
                str, task_instance.xcom_pull(task_ids="process_emails_recebidas") or ""
            )

            dfs = []
            if path_enviadas:
                dfs.append(pd.read_csv(path_enviadas))
            if path_recebidas:
                dfs.append(pd.read_csv(path_recebidas))

            if not dfs:
                logging.warning("Nenhum dado foi encontrado para combinar.")
                return ""

            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df["dt_ingest"] = datetime.now().isoformat()

            output_path = (
                f"/tmp/{context['dag'].dag_id}_nc_combinadas_{context['ts_nodash']}.csv"
            )
            combined_df.to_csv(output_path, index=False)

            logging.info(
                "Dados combinados: %s registros no total. Arquivo: %s",
                len(combined_df),
                output_path,
            )
            return output_path

        except Exception as e:
            logging.error(f"Erro ao combinar os dados: {str(e)}")
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        """
        Função para inserir os dados no banco de dados.
        """
        try:
            task_instance: Any = context["ti"]
            combined_path = task_instance.xcom_pull(task_ids="combine_data")

            if not combined_path:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            combined_df = pd.read_csv(combined_path)
            data = combined_df.to_dict(orient="records")

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            db.insert_data(data, "nc_tesouro", schema="siafi")
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
            db.remove_duplicates("nc_tesouro", COLUMN_MAPPING, schema="siafi")

        except Exception as e:
            logging.error(f"Erro ao executar a limpeza de duplicados: {str(e)}")
            raise

    # Tarefa 1: Processar os e-mails de notas de crédito enviadas/devolvidas
    process_emails_enviadas_task = PythonOperator(
        task_id="process_emails_enviadas",
        python_callable=process_email_data_enviadas,
        provide_context=True,
    )

    # Tarefa 2: Processar os e-mails de notas de crédito recebidas
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

    # Tarefa 4: Inserir os dados no banco de dados
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
