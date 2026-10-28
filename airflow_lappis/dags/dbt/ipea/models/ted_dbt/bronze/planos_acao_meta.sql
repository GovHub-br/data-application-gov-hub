-- Metas do Plano de Ação de TED (API de TED, endpoint plano_acao_meta).
with

    planos_acao_meta as (
        select distinct
            on ({{ safe_bigint("id_meta") }})
            {{ safe_bigint("id_meta") }} as id_meta,
            {{ safe_bigint("id_plano_acao") }} as id_plano_acao,
            {{ safe_bigint("nr_numero_meta") }} as nr_meta,
            {{ safe_text("tx_nome_meta") }} as no_meta,
            {{ safe_text("tx_descricao_meta") }} as ds_meta,
            {{ safe_text("tp_unidade_meta") }} as ds_unidade,
            {{ safe_numeric("nr_quantidade_meta") }} as qt_meta,
            {{ safe_numeric("vl_valor_unitario_meta") }} as vl_unitario,
            {{ safe_date("dt_inicio_vigencia_meta") }} as dt_inicio,
            {{ safe_date("dt_fim_vigencia_meta") }} as dt_fim,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "planos_acao_meta") }}
        order by {{ safe_bigint("id_meta") }}, dt_ingest desc
    )

select *
from planos_acao_meta
