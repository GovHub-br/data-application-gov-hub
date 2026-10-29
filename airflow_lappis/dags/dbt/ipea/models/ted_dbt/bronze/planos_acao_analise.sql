-- Análises do Plano de Ação de TED (API de TED, endpoint plano_acao_analise).
with

    planos_acao_analise as (
        select distinct
            on ({{ safe_bigint("id_analise") }})
            {{ safe_bigint("id_analise") }} as id_analise,
            {{ safe_bigint("id_plano_acao") }} as id_plano_acao,
            {{ safe_text("tx_justificativa_analise") }} as ds_justificativa,
            {{ safe_text("resultado_analise") }} as ds_resultado,
            {{ safe_text("tx_situacao_analise") }} as ds_situacao,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "planos_acao_analise") }}
        order by {{ safe_bigint("id_analise") }}, dt_ingest desc
    )

select *
from planos_acao_analise
