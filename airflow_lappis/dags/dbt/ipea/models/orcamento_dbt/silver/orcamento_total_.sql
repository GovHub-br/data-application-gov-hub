WITH orcamento_teds AS (
    select
        SUM(credito_disponivel) + SUM(despesas_empenhadas) as orcamento,
        ano_exercicio,
        max(dt_ingest) as dt_ingest
    from {{ ref('visao_orcamentaria_total') }}
    where unidade_orcamentaria not in ('25300', '47204')
    group by ano_exercicio
),

orcamento AS (
    select
        SUM(dotacao_atualizada) as orcamento,
        ano_exercicio,
        max(dt_ingest) as dt_ingest
    from {{ ref('visao_orcamentaria_total') }}
    group by ano_exercicio
),

orcamento_total AS (
    select * from orcamento_teds
    union
    select * from orcamento
)

select
    ano_exercicio,
    sum(orcamento) as orcamento,
    max(dt_ingest) as dt_ingest
from orcamento_total
group by ano_exercicio
