with

    orcamento_teds as (
        select
            sum(credito_disponivel) + sum(despesas_empenhadas) as orcamento,
            ano_exercicio,
            'TEDs' as tipo_orcamento,
            max(dt_ingest) as dt_ingest
        from {{ ref('visao_orcamentaria_total') }}
        where unidade_orcamentaria not in ('25300', '47204')
        group by ano_exercicio
    ),

    orcamento_geral as (
        select
            sum(dotacao_atualizada) as orcamento,
            ano_exercicio,
            'Geral' as tipo_orcamento,
            max(dt_ingest) as dt_ingest
        from {{ ref('visao_orcamentaria_total') }}
        group by ano_exercicio
    ),

    orcamento_total as (
        select * from orcamento_teds
        union all
        select * from orcamento_geral
    )

select
    ano_exercicio,
    sum(orcamento) as orcamento,
    max(dt_ingest) as dt_ingest
from orcamento_total
group by ano_exercicio
