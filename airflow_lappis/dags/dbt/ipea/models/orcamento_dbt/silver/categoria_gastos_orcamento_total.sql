with
    categorias_gastos as (
        select
            ano_exercicio,
            acao_governo,
            acao_governo_desc,
            elemento_despesa,
            elemento_despesa_desc,
            coalesce(dotacao_atualizada, 0) as valor,
            'Dotação' as categoria
        from {{ ref("visao_orcamentaria_total") }}
        union all
        select
            ano_exercicio,
            acao_governo,
            acao_governo_desc,
            elemento_despesa,
            elemento_despesa_desc,
            coalesce(despesas_empenhadas, 0) as valor,
            'Orçamento alocado (empenhado)' as categoria
        from {{ ref("visao_orcamentaria_total") }}
        union all
        select
            ano_exercicio,
            acao_governo,
            acao_governo_desc,
            elemento_despesa,
            elemento_despesa_desc,
            coalesce(despesar_a_pagar, 0) as valor,
            'Despesas programadas' as categoria
        from {{ ref("visao_orcamentaria_total") }}
        union all
        select
            ano_exercicio,
            acao_governo,
            acao_governo_desc,
            elemento_despesa,
            elemento_despesa_desc,
            coalesce(despesas_pagas, 0) as valor,
            'Despesas pagas' as categoria
        from {{ ref("visao_orcamentaria_total") }}
    )

select *
from categorias_gastos
order by valor desc
