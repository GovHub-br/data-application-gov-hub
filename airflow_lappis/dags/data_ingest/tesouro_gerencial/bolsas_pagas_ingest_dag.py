from datetime import datetime

from airflow import DAG
from email_ingest_dag_factory import build_email_ingest_dag

COLUMN_MAPPING = {
    0: "credor_codigo",
    1: "credor_nome",
    2: "dia_emissao",
    3: "mes_emissao",
    4: "ano_emissao",
    5: "emissao_ano",
    6: "mes_lancamento",
    7: "fonte_recursos_codigo",
    8: "fonte_recursos_descricao",
    9: "pi_codigo",
    10: "pi_descricao",
    11: "ptres",
    12: "natureza_codigo",
    13: "natureza_descricao",
    14: "processo",
    15: "valor",
    16: "observacao",
    17: "ne_ccor",
    18: "documento_habil",
    19: "item_informacao",
    20: "despesa_paga",
    21: "rp_processados",
    22: "rp_nao_processados",
    23: "pagamentos_totais",
}

dag: DAG = build_email_ingest_dag(
    dag_id="email_bolsas_pagas_tesouro_ingest",
    email_subject="bolsas_pagas_ipea",
    column_mapping=COLUMN_MAPPING,
    skiprows=11,
    table_name="bolsas_pagas",
    schedule_key="bolsas_pagas_ingest_dag",
    schema="siafi",
    owner="Davi",
    start_date=datetime(2023, 12, 1),
    tags=["email", "tesouro", "bolsas_pagas"],
    description=(
        "Processa anexos de bolsas pagas do Tesouro Gerencial recebidos por email "
        "e insere no banco"
    ),
)
