{{
    config(
        unique_key=[
            "ne_ccor",
            "natureza_despesa",
            "doc_observacao",
            "ne_ccor_ano_emissao",
            "emissao_dia",
            "emissao_mes",
            "despesas_empenhadas",
            "despesas_liquidadas",
            "despesas_pagas",
        ],
        incremental_strategy="merge",
    )
}}

with
    empenhos_raw as (
        select
            emissao_mes::text as emissao_mes,
            emissao_dia::text as emissao_dia,
            ne_ccor::text as ne_ccor,
            regexp_replace(ne_num_processo, '[./-]', '') as ne_num_processo,
            ne_info_complementar::text as ne_info_complementar,
            ne_ccor_descricao::text as ne_ccor_descricao,
            doc_observacao::text as doc_observacao,
            natureza_despesa::text as natureza_despesa,
            natureza_despesa_descricao::text as natureza_despesa_descricao,
            upper(ne_ccor_favorecido::text) as ne_ccor_favorecido,
            ne_ccor_favorecido_descricao::text as ne_ccor_favorecido_descricao,
            ne_ccor_ano_emissao::integer as ne_ccor_ano_emissao,
            ptres::text as ptres,
            fonte_recursos_detalhada::text as fonte_recursos_detalhada,
            fonte_recursos_detalhada_descricao::text
            as fonte_recursos_detalhada_descricao,
            {{ parse_financial_value("despesas_empenhadas") }} as despesas_empenhadas,
            {{ parse_financial_value("despesas_liquidadas") }} as despesas_liquidadas,
            {{ parse_financial_value("despesas_pagas") }} as despesas_pagas,
            {{ parse_financial_value("restos_a_pagar_inscritos") }}
            as restos_a_pagar_inscritos,
            {{ parse_financial_value("restos_a_pagar_pagos") }} as restos_a_pagar_pagos
        from {{ source("siafi", "empenhos_tesouro") }}
        where ne_ccor_ano_emissao like '20%'
    )

select *
from empenhos_raw
