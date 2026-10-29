-- Termo de Execução do TED (API de TED, endpoint termo_execucao): processo SEI e
-- datas de assinatura, divulgação, recebimento e efetivação.
with

    termos_execucao as (
        select distinct
            on ({{ safe_bigint("id_termo") }})
            {{ safe_bigint("id_termo") }} as id_termo,
            {{ safe_bigint("id_plano_acao") }} as id_plano_acao,
            {{ safe_text("tx_situacao_termo") }} as ds_situacao,
            {{ safe_text("tx_num_processo_sei") }} as nr_processo_sei,
            {{ safe_date("dt_assinatura_termo") }} as dt_assinatura,
            {{ safe_date("dt_divulgacao_termo") }} as dt_divulgacao,
            {{ safe_boolean("in_minuta_padrao") }} as in_minuta_padrao,
            {{ safe_text("tx_numero_ns_termo") }} as nr_ns,
            {{ safe_timestamp("dt_recebimento_termo") }}::date as dt_recebimento,
            {{ safe_date("dt_efetivacao_termo") }} as dt_efetivacao,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("transfere_gov", "termos_execucao") }}
        order by {{ safe_bigint("id_termo") }}, dt_ingest desc
    )

select *
from termos_execucao
