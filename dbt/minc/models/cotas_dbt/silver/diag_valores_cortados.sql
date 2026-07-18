-- Diagnóstico de valores por magnitude: audita o que o teto de R$10M cortou.
-- Faixas 'CORTADO*' viraram NULL em valor_pago_num (saem do denominador de valor).
with base as (
    select origem, valor_bruto_num from {{ ref('stg_contemplados_lpg') }}
    union all
    select origem, valor_bruto_num from {{ ref('stg_contemplados_pnab') }}
)
select
    origem,
    case
        when valor_bruto_num is null          then '1_sem_valor'
        when valor_bruto_num <= 10000000      then '2_ok_ate_10M'
        when valor_bruto_num <= 100000000     then '3_CORTADO_10M_100M'
        when valor_bruto_num <= 1000000000000 then '4_CORTADO_100M_1tri'
        else                                       '5_CORTADO_acima_1tri'
    end                                       as faixa_valor,
    count(*)                                  as qtd
from base
group by 1, 2
order by 1, 2
