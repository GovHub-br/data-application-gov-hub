from typing import Dict, Any, List
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models.param import Param
from datetime import datetime, timedelta
import logging
import json
from schedule_loader import get_dynamic_schedule
from cliente_email import fetch_email_with_zip, extract_csv_from_zip
from cliente_postgres import ClientPostgresDB
from postgres_helpers import get_postgres_conn

# Configurações básicas da DAG
default_args = {
    "owner": "Tiago",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

COLUMN_MAPPING = {
    0: "programa_governo",
    1: "programa_governo_descricao",
    2: "acao_governo",
    3: "acao_governo_descricao",
    4: "emissao_mes",
    5: "emissao_dia",
    6: "ne_ccor",
    7: "ne_num_processo",
    8: "ne_info_complementar",
    9: "ne_ccor_descricao",
    10: "doc_observacao",
    11: "natureza_despesa",
    12: "natureza_despesa_descricao",
    13: "ne_ccor_favorecido",
    14: "ne_ccor_favorecido_descricao",
    15: "ne_ccor_ano_emissao",
    16: "ptres",
    17: "fonte_recursos_detalhada",
    18: "fonte_recursos_detalhada_descricao",
    19: "despesas_empenhadas",
    20: "despesas_liquidadas",
    21: "despesas_pagas",
    22: "restos_a_pagar_inscritos",
    23: "restos_a_pagar_pagos",
}

UNIQUE_KEY = [
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

EMAIL_SUBJECT = "notas_de_empenho_ano_atual"
SKIPROWS = 8

# Configurações da DAG
with DAG(
    dag_id="email_tesouro_parlamentares_ingest",
    default_args=default_args,
    description="Processa anexos dos empenhos vindo do email, formata e insere no db",
    schedule_interval=get_dynamic_schedule("empenhos_tesouro_parlamentares_ingest_dag"),
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
    tags=["MIR", "email", "empenhos", "tesouro", "parlamentares"],
) as dag:

    def _get_db_client() -> ClientPostgresDB:
        return ClientPostgresDB(get_postgres_conn("postgres_mir"))

    def _insert_dataframe(df, db: ClientPostgresDB) -> int:
        df = df[df["ne_ccor_ano_emissao"].astype(str).str.startswith("20")]
        records = df.to_dict(orient="records")
        for r in records:
            r["dt_ingest"] = datetime.now().isoformat()

        db.insert_data(
            records,
            "empenhos_tesouro_parlamentares",
            conflict_fields=UNIQUE_KEY,
            primary_key=UNIQUE_KEY,
            schema="siafi",
        )
        return len(records)

    def fetch_and_ingest(**context: Dict[str, Any]) -> Dict[str, int]:
        """Processa cada anexo e ingere imediatamente, evitando acúmulo em memória."""
        creds = json.loads(Variable.get("email_credentials"))
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

        logging.info(
            "Buscando e-mails para a data: %s",
            target_date.isoformat() if target_date else "dia atual",
        )

        zip_payloads: List[bytes] = fetch_email_with_zip(
            creds["imap_server"],
            creds["email"],
            creds["password"],
            creds["sender_email"],
            EMAIL_SUBJECT,
            target_date=target_date,
        )

        if not zip_payloads:
            logging.warning("Nenhum anexo ZIP encontrado.")
            return {"attachments": 0, "records": 0}

        logging.info("Total de anexos ZIP encontrados: %s", len(zip_payloads))

        db = _get_db_client()
        total_records = 0

        for idx, payload in enumerate(zip_payloads, 1):
            df = extract_csv_from_zip(payload, COLUMN_MAPPING, SKIPROWS)
            if df is not None:
                count = _insert_dataframe(df, db)
                total_records += count
                logging.info("Anexo %s: %s registros inseridos", idx, count)
            else:
                logging.warning("Anexo %s ignorado (CSV inválido)", idx)
            del df  # Libera memória imediatamente

        logging.info("Total: %s anexos, %s registros", len(zip_payloads), total_records)
        return {"attachments": len(zip_payloads), "records": total_records}

    PythonOperator(
        task_id="fetch_and_ingest",
        python_callable=fetch_and_ingest,
    )
