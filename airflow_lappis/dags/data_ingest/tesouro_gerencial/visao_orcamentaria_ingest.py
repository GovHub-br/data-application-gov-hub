from typing import Dict, Any, Optional, List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
import pandas as pd
import io
import re
import zipfile
from cliente_email import fetch_email_with_zip
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

# Configurações básicas da DAG
default_args = {
    "owner": "Davi",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Mapeamento das colunas para visão orçamentária
COLUMN_MAPPING = {
    0: "unidade_orcamentaria",
    1: "unidade_orcamentaria_desc",
    2: "acao_governo",
    3: "acao_governo_desc",
    4: "programa_governo",
    5: "programa_governo_desc",
    6: "unidade_plano_orcamentario",
    7: "plano_orcamentario_1",
    8: "plano_orcamentario_2",
    9: "programa_plano_orcamentario",
    10: "acao_plano_orcamentario",
    11: "plano_orcamentario_6",
    12: "plano_orcamentario_desc",
    13: "elemento_despesa",
    14: "elemento_despesa_desc",
    15: "orgao_uge",
    16: "orgao_uge_desc",
    17: "uge_matriz_filial",
    18: "ug_executora",
    19: "ug_executora_desc",
    20: "projeto_inicial_loa",
    21: "dotacao_inicial",
    22: "dotacao_atualizada",
    23: "credito_disponivel",
    24: "despesas_empenhadas",
    25: "despesas_a_liquidar",
    26: "despesar_a_pagar",
    27: "despesas_pagas",
    28: "restos_a_pagar_inscritos",
    29: "restos_a_pagar_pagos",
}

EMAIL_SUBJECT = "visao_orcamentaria_total_ipea"

# Configurações da DAG
with DAG(
    dag_id="visao_orcamentaria_ingest",
    default_args=default_args,
    description=(
        "DAG processa anexos da visão orçamentária total IPEA "
        "vindo do email, formata e insere no db"
    ),
    schedule_interval="0 13 * * 1-6",
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=["email", "visao_orcamentaria", "tesouro"],
) as dag:

    def _is_valid_data_line(line: str, columns: List[str]) -> bool:
        """Verifica se a linha contém dados válidos para processamento."""
        # Verifica se é linha de cabeçalho, separador ou vazia
        header_indicators = [
            "Páginas:",
            "Unidade Orçamentária",
            '"Unidade Orçamentária"',
            '"UG Executora"',
            '"PROJETO INICIAL DA LOA"',
        ]

        if (
            not line
            or line.startswith(" ")
            or len(columns) < 20
            or any(indicator in line for indicator in header_indicators)
        ):
            return False

        # Verifica se tem dados essenciais preenchidos
        unidade_orc = columns[0].strip('"').strip() if columns else ""
        elemento_desp = columns[13].strip('"').strip() if len(columns) > 13 else ""

        return bool(
            unidade_orc
            and unidade_orc not in ["", '""']
            and elemento_desp
            and elemento_desp not in ["", '""']
        )

    def _process_data_block(data_lines: List[str], year: str) -> List[Dict[str, Any]]:
        """Processa um bloco de dados de um ano específico."""
        block_data = []

        for line in data_lines:
            line = line.strip()
            columns = line.split("\t")

            if not _is_valid_data_line(line, columns):
                continue

            # Remove aspas e limpa colunas
            columns = [col.strip('"').strip() for col in columns]

            # Cria registro com mapeamento de colunas
            row_data = {
                col_name: columns[col_index] if col_index < len(columns) else ""
                for col_index, col_name in COLUMN_MAPPING.items()
            }
            row_data["ano_exercicio"] = year
            block_data.append(row_data)

        return block_data

    def _parse_csv_by_year_blocks(csv_content: str) -> List[Dict[str, Any]]:
        """
        Processa CSV organizando dados por blocos de ano.

        Lógica:
        1. Encontra linhas com "Ano Lançamento: XXXX"
        2. Pula as próximas 5 linhas (cabeçalhos)
        3. Processa os dados até encontrar o próximo bloco ou fim do arquivo
        4. Adiciona a coluna ano_exercicio com o ano extraído
        """
        lines = csv_content.strip().split("\n")
        processed_data = []
        current_year = None
        data_start_index = None

        logging.info(f"Iniciando processamento do CSV com {len(lines)} linhas")

        for i, line in enumerate(lines):
            year_match = re.search(r"Ano Lançamento:\s*(\d{4})", line)

            if year_match:
                # Processa bloco anterior se existir
                if current_year and data_start_index is not None:
                    data_lines = lines[
                        data_start_index : i - 2
                    ]  # Deixa margem para linhas vazias
                    year_data = _process_data_block(data_lines, current_year)
                    logging.info(
                        f"Processados {len(year_data)} registros para o ano "
                        f"{current_year}"
                    )
                    processed_data.extend(year_data)

                # Inicia novo bloco
                current_year = year_match.group(1)
                data_start_index = i + 6  # Pula 5 linhas após "Ano Lançamento:"
                logging.info(
                    f"Iniciando processamento do ano {current_year} a partir da "
                    f"linha {data_start_index}"
                )

        # Processa o último bloco
        if current_year and data_start_index is not None:
            data_lines = lines[data_start_index:]
            year_data = _process_data_block(data_lines, current_year)
            logging.info(
                f"Processados {len(year_data)} registros para o último ano "
                f"{current_year}"
            )
            processed_data.extend(year_data)

        logging.info(
            f"Processamento concluído. Total de registros: {len(processed_data)}"
        )
        return processed_data

    def process_email_data(**context: Dict[str, Any]) -> Optional[str]:
        """Processa o email e retorna os dados formatados."""
        creds = json.loads(Variable.get("email_credentials"))

        try:
            logging.info("Iniciando o processamento dos emails...")

            # Busca o email com attachments ZIP
            zip_payload = fetch_email_with_zip(
                creds["imap_server"],
                creds["email"],
                creds["password"],
                creds["sender_email"],
                EMAIL_SUBJECT,
            )

            if not zip_payload:
                logging.warning("Nenhum e-mail encontrado com o assunto esperado.")
                return None

            # Extrai o CSV do ZIP (UTF-16)
            with zipfile.ZipFile(io.BytesIO(zip_payload)) as zip_file:
                for file_name in zip_file.namelist():
                    if file_name.endswith(".csv"):
                        raw_data = zip_file.read(file_name)
                        csv_content = raw_data.decode("utf-16")
                        break
                else:
                    logging.warning("Nenhum arquivo CSV encontrado no ZIP.")
                    return None

            # Processa o CSV com a lógica de blocos por ano
            processed_data = _parse_csv_by_year_blocks(csv_content)

            if not processed_data:
                logging.warning("Nenhum dado foi processado do CSV.")
                return None

            # Converte para CSV string
            df = pd.DataFrame(processed_data)
            csv_string = df.to_csv(index=False)

            logging.info(
                f"CSV processado com sucesso. Dados encontrados: "
                f"{len(processed_data)} registros"
            )
            return csv_string

        except Exception as e:
            logging.error(f"Erro no processamento dos emails: {str(e)}")
            raise

    def insert_and_clean_data(**context: Dict[str, Any]) -> None:
        """Insere os dados no banco e limpa duplicados."""
        try:
            task_instance: Any = context["ti"]
            csv_data: Any = task_instance.xcom_pull(task_ids="process_emails")

            if not csv_data:
                logging.warning("Nenhum dado para inserir no banco.")
                return

            df = pd.read_csv(io.StringIO(csv_data))
            data = df.to_dict(orient="records")

            postgres_conn_str = get_postgres_conn()
            db = ClientPostgresDB(postgres_conn_str)

            # Insere os dados
            db.insert_data(
                data,
                "visao_orcamentaria_total",
                schema="siafi",
            )

            logging.info(
                f"Dados inseridos com sucesso no banco de dados. Total: "
                f"{len(data)} registros"
            )

            # Remove duplicados
            column_mapping_with_year = {**COLUMN_MAPPING, 30: "ano_exercicio"}
            db.remove_duplicates(
                "visao_orcamentaria_total_ipea", column_mapping_with_year, schema="siafi"
            )

            logging.info("Limpeza de duplicados concluída com sucesso.")

        except Exception as e:
            logging.error(f"Erro ao inserir dados ou limpar duplicados: {str(e)}")
            raise

    # Definição das tarefas
    process_emails_task = PythonOperator(
        task_id="process_emails",
        python_callable=process_email_data,
        provide_context=True,
    )

    insert_and_clean_task = PythonOperator(
        task_id="insert_and_clean",
        python_callable=insert_and_clean_data,
        provide_context=True,
    )

    # Fluxo da DAG
    process_emails_task >> insert_and_clean_task
