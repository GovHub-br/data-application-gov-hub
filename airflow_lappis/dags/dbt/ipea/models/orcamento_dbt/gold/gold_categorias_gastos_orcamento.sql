select
    v.ano_exercicio,
    v.acao_governo,
    v.acao_governo_desc,
    v.elemento_despesa,
    v.elemento_despesa_desc,
    coalesce(u.valor, 0) as valor,
    u.categoria,
    v.dt_ingest
from {{ ref('visao_orcamentaria_total') }} as v
cross join lateral (
    values
        (v.dotacao_atualizada, 'Dotação'),
        (v.despesas_empenhadas, 'Orçamento alocado (empenhado)'),
        (v.despesar_a_pagar, 'Despesas programadas'),
        (v.despesas_pagas, 'Despesas pagas')
) as u(valor, categoria)
order by valor desc
