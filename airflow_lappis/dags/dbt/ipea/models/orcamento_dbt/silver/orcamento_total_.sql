with
    orcamento_teds as (
        select
            sum(credito_disponivel) + sum(despesas_empenhadas) as orcamento,
            ano_exercicio,
            max(dt_ingest) as dt_ingest
        from {{ ref("visao_orcamentaria_total") }}
        where unidade_orcamentaria not in ('25300', '47204')
        group by ano_exercicio
    ),

    orcamento as (
        select
            sum(dotacao_atualizada) as orcamento,
            ano_exercicio,
            max(dt_ingest) as dt_ingest
        from {{ ref("visao_orcamentaria_total") }}
        group by ano_exercicio
    ),

    orcamento_total as (
        select *
        from orcamento_teds
        union
        select *
        from orcamento
    )

select ano_exercicio, sum(orcamento) as orcamento, max(dt_ingest) as dt_ingest
from orcamento_total
group by ano_exercicio
