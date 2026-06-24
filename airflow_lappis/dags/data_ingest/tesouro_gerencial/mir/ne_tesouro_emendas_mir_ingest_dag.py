from typing import Any, Dict, List, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.param import Param
from datetime import datetime, timedelta
import csv
import io
import json
import logging
import cliente_email
from schedule_loader import get_dynamic_schedule
from cliente_email import fetch_and_process_email
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn
import pandas as pd

default_args = {
    "owner": "Tiago",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TABLE_SCHEMA = "siafi"
TABLE_NAME = "ne_tesouro_emendas"
EMAIL_SUBJECT = "notas_de_empenhos_emendas_parlamentares"

TARGET_COLUMNS: List[str] = [
    "emissao_mes",
    "emissao_dia",
    "programa_governo",
    "programa_governo_descricao",
    "acao_governo",
    "acao_governo_descricao",
    "autor_emendas_orcamento",
    "autor_emendas_orcamento_descricao",
    "localizador_gasto",
    "localizador_gasto_descricao",
    "regiao_pt",
    "uf_pt",
    "uf_pt_descricao",
    "municipio_pt",
    "ne_ccor",
    "ne_num_processo",
    "ne_info_complementar",
    "ne_ccor_descricao",
    "doc_observacao",
    "grupo_despesa",
    "grupo_despesa_descricao",
    "natureza_despesa",
    "natureza_despesa_descricao",
    "modalidade_aplicacao",
    "modalidade_aplicacao_descricao",
    "ne_ccor_favorecido",
    "ne_ccor_favorecido_descricao",
    "ne_ccor_ano_emissao",
    "ptres",
    "fonte_recursos_detalhada",
    "fonte_recursos_detalhada_descricao",
    "despesas_empenhadas",
    "despesas_liquidadas",
    "despesas_pagas",
    "restos_a_pagar_inscritos",
    "restos_a_pagar_pagos",
]

HEADER_TO_CANONICAL: Dict[str, str] = {
    "Emissão - Mês": "emissao_mes",
    "Emissão - Dia": "emissao_dia",
    "Programa Governo": "programa_governo",
    "Ação Governo": "acao_governo",
    "Autor Emendas Orçamento": "autor_emendas_orcamento",
    "Localizador Gasto": "localizador_gasto",
    "Região PT": "regiao_pt",
    "UF PT": "uf_pt",
    "Município PT": "municipio_pt",
    "NE CCor": "ne_ccor",
    "NE - Núm. Processo": "ne_num_processo",
    "NE - Informação Complementar": "ne_info_complementar",
    "NE CCor - Descrição": "ne_ccor_descricao",
    "Doc - Observação": "doc_observacao",
    "Grupo Despesa": "grupo_despesa",
    "Natureza Despesa": "natureza_despesa",
    "Modalidade Aplicação": "modalidade_aplicacao",
    "NE CCor - Favorecido": "ne_ccor_favorecido",
    "NE CCor - Ano Emissão": "ne_ccor_ano_emissao",
    "PTRES": "ptres",
    # Header diz "Item Informação" mas a coluna traz o codigo da fonte de
    # recursos (ex.: 1000000000 = RECURSOS LIVRES DA UNIAO).
    "Item Informação": "fonte_recursos_detalhada",
}

HEADERS_WITH_DESCRICAO = {
    "Programa Governo",
    "Ação Governo",
    "Autor Emendas Orçamento",
    "Localizador Gasto",
    "UF PT",
    "Grupo Despesa",
    "Natureza Despesa",
    "Modalidade Aplicação",
    "NE CCor - Favorecido",
    "Item Informação",
}

ITEM_CODE_TO_CANONICAL = {
    "29": "despesas_empenhadas",
    "31": "despesas_liquidadas",
    "34": "despesas_pagas",
    "50": "restos_a_pagar_inscritos",
    "52": "restos_a_pagar_pagos",
}

HEADER_MARKER = '"Emissão - Mês"'
SUB_HEADER_LINES = 2

# Coluna do schema antigo (posicional, quebrado) usada para detectar
# tabelas que precisam ser recriadas. Ver reset_table_if_legacy_schema.
LEGACY_SCHEMA_MARKER_COLUMN = "item_informacao"

# Cada linha do relatorio e uma movimentacao contabil do empenho
# (emissao, alteracao, anulacao, pagamento). doc_observacao distingue
# as movimentacoes do mesmo empenho no mesmo dia.
UNIQUE_KEY = ["ne_ccor", "emissao_mes", "emissao_dia", "doc_observacao"]


def _build_column_map(header_row: List[str]) -> Dict[str, int]:
    col_map: Dict[str, int] = {}
    for pos, raw in enumerate(header_row):
        name = raw.strip()
        if name in HEADER_TO_CANONICAL:
            canonical = HEADER_TO_CANONICAL[name]
            col_map[canonical] = pos
            if name in HEADERS_WITH_DESCRICAO:
                col_map[f"{canonical}_descricao"] = pos + 1
        elif name in ITEM_CODE_TO_CANONICAL:
            col_map[ITEM_CODE_TO_CANONICAL[name]] = pos
    return col_map


def parse_tesouro_emendas_csv(
    csv_data: str,
    column_mapping: Optional[Dict[int, str]] = None,
    skiprows: int = 0,
) -> pd.DataFrame:
    """Parser do relatorio do Tesouro Gerencial de empenhos de emendas.

    O arquivo concatena multiplos sub-relatorios (um por "Ano Lançamento"),
    cada um com seu proprio cabecalho e numero de colunas financeiras
    diferente. O parser detecta cada sub-relatorio pelo cabecalho
    repetido e mapeia colunas pelo nome (nao por posicao).

    `column_mapping` e `skiprows` sao ignorados — existem so para manter
    a assinatura de cliente_email.format_csv, que esta funcao substitui
    via monkey-patch.
    """
    del column_mapping, skiprows

    lines = csv_data.splitlines()
    header_indices = [
        i for i, line in enumerate(lines)
        if line.lstrip().startswith(HEADER_MARKER)
    ]
    if not header_indices:
        raise ValueError(
            "Nenhum cabecalho de empenhos de emendas encontrado no CSV — "
            f"esperava linhas comecando com {HEADER_MARKER}."
        )

    sub_report_ranges = [
        (start, header_indices[idx + 1] if idx + 1 < len(header_indices) else len(lines))
        for idx, start in enumerate(header_indices)
    ]

    records: List[Dict[str, Any]] = []
    for sub_idx, (h_start, h_end) in enumerate(sub_report_ranges, start=1):
        header_row = next(csv.reader([lines[h_start]]))
        col_map = _build_column_map(header_row)
        expected_width = len(header_row)
        data_start = h_start + 1 + SUB_HEADER_LINES

        ne_pos = col_map.get("ne_ccor")
        if ne_pos is None:
            logging.warning(
                "Sub-relatorio %s: cabecalho sem coluna 'NE CCor'; ignorando.",
                sub_idx,
            )
            continue

        financial_present = sorted(
            c for c in col_map if c in ITEM_CODE_TO_CANONICAL.values()
        )
        kept = 0
        for line in lines[data_start:h_end]:
            if not line.strip():
                continue
            try:
                row = next(csv.reader([line]))
            except csv.Error:
                continue
            if len(row) != expected_width:
                continue
            if ne_pos >= len(row) or not row[ne_pos].strip():
                continue

            record: Dict[str, Any] = {}
            for canonical, pos in col_map.items():
                if pos < len(row):
                    value = row[pos].strip()
                    record[canonical] = value if value else None
            records.append(record)
            kept += 1

        logging.info(
            "Sub-relatorio %s: %s colunas, %s linhas, financeiras: %s",
            sub_idx,
            expected_width,
            kept,
            ", ".join(financial_present) or "(nenhuma)",
        )

    df = pd.DataFrame(records, columns=TARGET_COLUMNS)
    logging.info("Parser concluido: %s linhas no schema canonico.", len(df))
    return df


def reset_table_if_legacy_schema(db: ClientPostgresDB) -> None:
    """Dropa a tabela se ainda estiver no schema antigo.

    O schema antigo lia colunas por posicao fixa e gravava valores de
    Restos a Pagar como despesas_liquidadas/pagas em sub-relatorios de
    34 colunas. Os dados existentes estao semanticamente corrompidos —
    recarregar do zero e a unica correcao segura.
    """
    rows = db.execute_query(
        f"SELECT 1 FROM information_schema.columns "
        f"WHERE table_schema = '{TABLE_SCHEMA}' "
        f"AND table_name = '{TABLE_NAME}' "
        f"AND column_name = '{LEGACY_SCHEMA_MARKER_COLUMN}' LIMIT 1;"
    )
    if not rows:
        return

    logging.warning(
        "Schema antigo detectado em %s.%s — dropando para recriar limpo.",
        TABLE_SCHEMA,
        TABLE_NAME,
    )
    db.execute_non_query(
        f"DROP TABLE IF EXISTS {TABLE_SCHEMA}.{TABLE_NAME} CASCADE;"
    )


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

        cliente_email.format_csv = parse_tesouro_emendas_csv

        try:
            logging.info(
                "Iniciando processamento dos emails para a data: %s",
                target_date.isoformat() if target_date else "dia atual",
            )
            csv_data = fetch_and_process_email(
                IMAP_SERVER,
                EMAIL,
                PASSWORD,
                SENDER_EMAIL,
                EMAIL_SUBJECT,
                column_mapping={},
                skiprows=0,
                target_date=target_date,
            )
            if not csv_data:
                logging.warning("Nenhum CSV valido foi extraido dos e-mails.")
                return None

            logging.info("CSV processado: %s caracteres.", len(csv_data))
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
                return

            df = pd.read_csv(io.StringIO(csv_data), dtype=str, keep_default_na=False)
            df = df.replace({"": None})
            df = df[df["ne_ccor_ano_emissao"].fillna("").str.startswith("20")]

            data = df.where(pd.notnull(df), None).to_dict(orient="records")
            for record in data:
                record["dt_ingest"] = datetime.now().isoformat()

            postgres_conn_str = get_postgres_conn("postgres_mir")
            db = ClientPostgresDB(postgres_conn_str)

            reset_table_if_legacy_schema(db)

            db.insert_data(
                data,
                TABLE_NAME,
                conflict_fields=UNIQUE_KEY,
                primary_key=UNIQUE_KEY,
                schema=TABLE_SCHEMA,
            )
            logging.info(
                "Inseridos %s registros em %s.%s.",
                len(data),
                TABLE_SCHEMA,
                TABLE_NAME,
            )
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
