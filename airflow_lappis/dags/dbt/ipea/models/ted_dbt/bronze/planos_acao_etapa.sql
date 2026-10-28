-- Etapas das metas do Plano de Ação de TED (API de TED, endpoint plano_acao_etapa).
with

    planos_acao_etapa as (
        select distinct
            on ({{ safe_bigint("id_etapa") }})
            {{ safe_bigint("id_etapa") }} as id_etapa,
            {{ safe_bigint("id_meta") }} as id_meta,
            {{ safe_bigint("nr_numero_etapa") }} as nr_etapa,
            {{ safe_text("tx_nome_etapa") }} as no_etapa,
            {{ safe_text("tx_descricao_etapa") }} as ds_etapa,
            {{ safe_text("unidade_medida_etapa") }} as ds_unidade,
            {{ safe_numeric("nr_quantidade_etapa") }} as qt_etapa,
            {{ safe_numeric("vl_valor_unitario_etapa") }} as vl_unitario,
            to_date(
                {{ safe_text("dt_inicio_vigencia_etapa") }}, 'YYYY-MM-DD'
            ) as dt_inicio,
            to_date({{ safe_text("dt_fim_vigencia_etapa") }}, 'YYYY-MM-DD') as dt_fim,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "planos_acao_etapa") }}
        order by {{ safe_bigint("id_etapa") }}, dt_ingest desc
    )

select *
from planos_acao_etapa
