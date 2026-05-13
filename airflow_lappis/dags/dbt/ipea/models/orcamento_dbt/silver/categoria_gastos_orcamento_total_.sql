WITH categorias_gastos AS (
    select
        ano_exercicio,
        acao_governo,
        acao_governo_desc,
        elemento_despesa,
        elemento_despesa_desc,
        COALESCE(dotacao_atualizada, 0) as valor,
        'Dotação' as categoria,
        dt_ingest
    from {{ ref('visao_orcamentaria_total') }}

    union all

    select
        ano_exercicio,
        acao_governo,
        acao_governo_desc,
        elemento_despesa,
        elemento_despesa_desc,
        COALESCE(despesas_empenhadas, 0) as valor,
        'Orçamento alocado (empenhado)' as categoria,
        dt_ingest
    from {{ ref('visao_orcamentaria_total') }}

    union all

    select
        ano_exercicio,
        acao_governo,
        acao_governo_desc,
        elemento_despesa,
        elemento_despesa_desc,
        COALESCE(despesar_a_pagar, 0) as valor,
        'Despesas programadas' as categoria,
        dt_ingest
    from {{ ref('visao_orcamentaria_total') }}

    union all

    select
        ano_exercicio,
        acao_governo,
        acao_governo_desc,
        elemento_despesa,
        elemento_despesa_desc,
        COALESCE(despesas_pagas, 0) as valor,
        'Despesas pagas' as categoria,
        dt_ingest
    from {{ ref('visao_orcamentaria_total') }}
)

select * from categorias_gastos
order by valor desc
