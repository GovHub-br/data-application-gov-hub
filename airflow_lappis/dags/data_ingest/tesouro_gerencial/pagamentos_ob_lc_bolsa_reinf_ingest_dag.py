from datetime import datetime

from airflow import DAG
from email_ingest_dag_factory import build_email_ingest_dag

COLUMN_MAPPING = {
    0: "dh_mes_emissao",
    1: "dh_dia_emissao",
    2: "dh_dia_pagamento",
    3: "emitente_ug_codigo",
    4: "emitente_ug_nome",
    5: "ob",
    6: "ob_lista_credores",
    7: "oblc_favorecido_numero",
    8: "oblc_favorecido_nome",
    9: "oblc_banco_codigo",
    10: "oblc_banco_nome",
    11: "oblc_agencia_bancaria_codigo",
    12: "oblc_agencia_bancaria_nome",
    13: "oblc_conta_bancaria",
    14: "oblc_valor",
    15: "oblc_sequencial",
    16: "ob_processo",
    17: "documento_habil",
    18: "doc_observacao",
}

dag: DAG = build_email_ingest_dag(
    dag_id="email_pagamentos_ob_lc_bolsa_reinf_tesouro_ingest",
    email_subject="Pagamentos _OB_LC_ BOLSA_REINF",
    column_mapping=COLUMN_MAPPING,
    skiprows=9,
    table_name="pagamentos_ob_bolsa",
    schedule_key="pagamentos_ob_lc_bolsa_reinf_ingest_dag",
    schema="siafi",
    owner="Davi",
    start_date=datetime(2023, 12, 1),
    tags=["email", "tesouro", "pagamentos", "bolsa_reinf"],
    description=(
        "Processa anexos de pagamentos OB/LC bolsa REINF do Tesouro Gerencial "
        "recebidos por email e insere no banco"
    ),
)
