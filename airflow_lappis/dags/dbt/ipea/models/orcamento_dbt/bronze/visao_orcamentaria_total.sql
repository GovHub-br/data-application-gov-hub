with
    source_data as (select * from {{ source("siafi", "visao_orcamentaria_total") }}),

    typed_data as (
        select
            ano_exercicio,
            unidade_orcamentaria,
            unidade_orcamentaria_desc,
            acao_governo,
            acao_governo_desc,
            programa_governo,
            programa_governo_desc,
            unidade_plano_orcamentario,
            plano_orcamentario_1,
            plano_orcamentario_2,
            programa_plano_orcamentario,
            acao_plano_orcamentario,
            plano_orcamentario_6,
            plano_orcamentario_desc,
            elemento_despesa,
            elemento_despesa_desc,
            orgao_uge,
            orgao_uge_desc,
            uge_matriz_filial,
            ug_executora,
            ug_executora_desc,

            -- Campos financeiros/monetários
            {{ parse_financial_value("projeto_inicial_loa") }} as projeto_inicial_loa,
            {{ parse_financial_value("dotacao_inicial") }} as dotacao_inicial,
            {{ parse_financial_value("dotacao_atualizada") }} as dotacao_atualizada,
            {{ parse_financial_value("credito_disponivel") }} as credito_disponivel,
            {{ parse_financial_value("despesas_empenhadas") }} as despesas_empenhadas,
            {{ parse_financial_value("despesas_a_liquidar") }} as despesas_a_liquidar,
            {{ parse_financial_value("despesar_a_pagar") }} as despesar_a_pagar,
            {{ parse_financial_value("despesas_pagas") }} as despesas_pagas,
            {{ parse_financial_value("restos_a_pagar_inscritos") }}
            as restos_a_pagar_inscritos,
            {{ parse_financial_value("restos_a_pagar_pagos") }} as restos_a_pagar_pagos,

            dt_ingest::timestamp as dt_ingest

        from source_data
    )

select *
from typed_data
