-- Cobertura por ano: teto de confiabilidade das cotas (ler ANTES das cotas).
-- ano_final NULL (= sem ano derivado) é rotulado 'sem_ano' para leitura direta.
-- Pessoas: qtd_pessoas (docs distintos, mascarados já viram NULL) vs com perfil.
-- Percentuais em % (0-100).
select
    coalesce(ano_final, 'sem_ano')                                   as ano_final,
    count(*)                                                          as qtd_pagamentos,
    count(distinct identificador_unico)                              as qtd_pessoas,
    count(distinct identificador_unico) filter (where tem_perfil)    as qtd_pessoas_com_perfil,
    sum(valor_pago_num)                                              as valor_total,
    sum(valor_pago_num) filter (where tem_perfil)                   as valor_com_perfil,
    round(
        count(distinct identificador_unico) filter (where tem_perfil)::numeric
        / nullif(count(distinct identificador_unico), 0) * 100, 2)  as cobertura_pessoas_pct,
    round(
        sum(valor_pago_num) filter (where tem_perfil)
        / nullif(sum(valor_pago_num), 0) * 100, 2)                  as cobertura_valor_pct,
    round(
        sum(valor_pago_num) filter (where origem_ano <> 'sem_ano')
        / nullif(sum(valor_pago_num), 0) * 100, 2)                  as cobertura_temporal_pct
from {{ ref('fct_pagamentos_elegiveis') }}
group by coalesce(ano_final, 'sem_ano')
order by coalesce(ano_final, 'sem_ano')
