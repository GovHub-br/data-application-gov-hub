with
    orcamento_teds as (
        select
            sum(credito_disponivel) + sum(despesas_empenhadas) as orcamento, ano_exercicio
        from {{ ref("visao_orcamentaria_total") }}
        where unidade_orcamentaria not in ('25300', '47204')
        group by ano_exercicio
    ),

    orcamento as (
        select sum(dotacao_atualizada) as orcamento, ano_exercicio
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

select ano_exercicio, sum(orcamento) as sum
from orcamento_total
group by ano_exercicio
