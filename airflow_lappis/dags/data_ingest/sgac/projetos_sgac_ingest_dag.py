from typing import Dict, Any, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
from schedule_loader import get_dynamic_schedule
from cliente_email import fetch_and_process_email_csv_attachment
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
import pandas as pd
import io

# Configurações básicas da DAG
default_args = {
    "owner": "Wallyson",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COLUMN_MAPPING = {
    0: "odata_etag",
    1: "id_interno_item",
    2: "id",
    3: "titulo",
    4: "entidades_externas",
    5: "instrumento",
    6: "instrumento_id",
    7: "diretoria_responsavel",
    8: "diretoria_responsavel_id",
    9: "objeto",
    10: "data_inicio",
    11: "data_vencimento",
    12: "total_de_recursos",
    13: "numero_do_proc",
    14: "coordenador",
    15: "coordenador_tipo_odata",
    16: "coordenador_claims",
    17: "coordenador_claims_tipo_odata",
    18: "nacionalidade",
    19: "nacionalidade_tipo_odata",
    20: "nacionalidade_id",
    21: "nacionalidade_id_tipo_odata",
    22: "recursos_orcament_x00",
    23: "recursos_orcament_x0",
    24: "status",
    25: "status_id",
    26: "eixo_tematico",
    27: "eixo_tematico_tipo_odata",
    28: "eixo_tematico_id",
    29: "eixo_tematico_id_tipo_odata",
    30: "predecessores",
    31: "predecessores_tipo_odata",
    32: "predecessores_id",
    33: "predecessores_id_tipo_odata",
    34: "prioridade",
    35: "prioridade_id",
    36: "justificativa",
    37: "objetivo_s_ge",
    38: "equipe_tecnica",
    39: "equipe_tecnica_tipo_odata",
    40: "equipe_tecnica_claims",
    41: "equipe_tecnica_claims_tipo_odata",
    42: "codigo",
    43: "unidades_envolvidas",
    44: "unidades_envolvidas_tipo_odata",
    45: "unidades_envolvidas_id",
    46: "unidades_envolvidas_id_tipo_odata",
    47: "historico_observa_x0",
    48: "a_solicitacao",
    49: "a_solicitacao_tipo_odata",
    50: "a_solicitacao_id",
    51: "a_solicitacao_id_tipo_odata",
    52: "modificado",
    53: "criado",
    54: "autor",
    55: "autor_claims",
    56: "editor",
    57: "editor_claims",
    58: "identificador",
    59: "eh_pasta",
    60: "miniatura",
    61: "link",
    62: "nome",
    63: "nome_arquivo_com_extensao",
    64: "caminho",
    65: "caminho_completo",
    66: "tipo_conteudo",
    67: "tipo_conteudo_id",
    68: "possui_anexos",
    69: "numero_versao",
    70: "aprovacao",
    71: "termos_aditivos",
    72: "equipe",
    73: "percentual_concluido",
    74: "corpo",
    75: "fiscal_e_substituto",
    76: "numero_siafi",
    77: "atribuido_a",
    78: "atribuido_a_claims"
}

EMAIL_SUBJECT = "SGAC"
SKIPROWS = 1

# Configurações da DAG
with DAG(
    dag_id="email_projetos_sgac_ingest",
    default_args=default_args,
    description="Processa anexos do email de dados do SGAC e insere no db",
    schedule_interval=get_dynamic_schedule("email_projetos_sgac_ingest"),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "projetos", "sgac"],
) as dag:

    def process_email_data(**context: Dict[str, Any]) -> Optional[Any]:
        creds = json.loads(Variable.get("email_credentials"))

        EMAIL = creds["email"]
        PASSWORD = creds["password"]
        IMAP_SERVER = creds["imap_server"]
        SENDER_EMAIL = Variable.get("sender_email_sgac", default_var=creds["sender_email"],)

        try:
            logging.info("Iniciando o processamento dos emails...")
            csv_data = fetch_and_process_email_csv_attachment(
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
                return None

            total_linhas = max(len(csv_data.splitlines()) - 1, 0)
            logging.info(
                "CSV processado com sucesso. Dados encontrados: %s", total_linhas
            )
            return csv_data
        except Exception as e:
            logging.error("Erro no processamento dos emails: %s", str(e))
            raise

    def insert_data_to_db(**context: Dict[str, Any]) -> None:
        """Insere no Postgres os dados retornados pela task de processamento do e-mail."""
        try:
            task_instance: Any = context["ti"]
            csv_data: Any = task_instance.xcom_pull(task_ids="process_emails")

            if not csv_data:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            df = pd.read_csv(io.StringIO(csv_data))
            if df.empty:
                logging.warning("CSV recebido sem registros para insercao.")
                return

            data = df.to_dict(orient="records")
            # Adiciona timestamp de ingestão a cada registro
            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            unique_key = ["id"]

            db.insert_data(
                data,
                "projetos_sgac",
                conflict_fields=unique_key,
                primary_key=unique_key,
                schema="sgac",
            )
            logging.info("Dados inseridos com sucesso no banco de dados.")
        except Exception as e:
            logging.error("Erro ao inserir dados no banco: %s", str(e))
            raise

    #tarefa 1: processar os e-mails e extrair o CSV
    process_emails_task = PythonOperator(
        task_id="process_emails",
        python_callable=process_email_data,
        provide_context=True,
    )
    #tarefa 2: inserir os dados no banco de dados
    insert_to_db_task = PythonOperator(
        task_id="insert_to_db",
        python_callable=insert_data_to_db,
        provide_context=True,
    )
    #Fluxo da DAG
    process_emails_task >> insert_to_db_task

    