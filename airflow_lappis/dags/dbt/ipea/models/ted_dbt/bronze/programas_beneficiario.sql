-- Beneficiários (órgãos, por código Siorg) dos programas de TED (API de TED,
-- endpoint programa_beneficiario). A fonte não tem chave; a ingestão substitui a
-- tabela inteira a cada carga.
with

    programas_beneficiario as (
        select
            {{ safe_bigint("id_programa") }} as id_programa,
            {{ safe_text("tx_codigo_siorg") }} as cd_siorg,
            {{ safe_text("tx_nome_beneficiario") }} as no_beneficiario,
            {{ safe_numeric("vl_valor_beneficiario") }} as vl_beneficiario,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "programas_beneficiario") }}
    )

select *
from programas_beneficiario
