-- Pareceres do Plano de Ação de TED (API de TED, endpoint plano_acao_parecer).
with

    planos_acao_parecer as (
        select distinct
            on ({{ safe_bigint("id_parecer") }})
            {{ safe_bigint("id_parecer") }} as id_parecer,
            {{ safe_bigint("id_plano_acao") }} as id_plano_acao,
            {{ safe_text("tp_analise_parecer") }} as tp_analise,
            {{ safe_text("resultado_parecer") }} as ds_resultado,
            {{ safe_text("tx_parecer") }} as ds_parecer,
            {{ safe_bigint("plano_acao_hist_fk") }} as id_plano_acao_hist,
            to_date({{ safe_text("dt_data_parecer") }}, 'YYYY-MM-DD') as dt_parecer,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "planos_acao_parecer") }}
        order by {{ safe_bigint("id_parecer") }}, dt_ingest desc
    )

select *
from planos_acao_parecer
