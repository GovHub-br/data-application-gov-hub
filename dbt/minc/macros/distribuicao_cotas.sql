{% macro distribuicao_cotas(programa, incluir_territorio=false) %}
{#-
  Distribuição de cotas por ano p/ UM programa (LPG ou PNAB), valor ponderado.
  Totais do ano via CTE sobre o fato filtrado (NÃO window no explodido — evita
  multiplicar por sobreposição de cotas). Reporta pct sobre total pago E sobre
  total-com-perfil; veredito vs meta.

  Args:
    programa: 'LPG' ou 'PNAB' — filtra fct.nome_programa.
    incluir_territorio: só LPG tem flag_territorio_vulneravel (agente c/ cidade/uf);
      no PNAB o flag é sempre NULL, então a 4ª cota (20%) só faz sentido p/ LPG.
-#}
with fct as (
    select * from {{ ref('fct_pagamentos_elegiveis') }}
    where nome_programa = '{{ programa }}'
),
totais as (
    select
        ano_final,
        sum(valor_pago_num)                            as valor_total_ano,
        sum(valor_pago_num) filter (where tem_perfil)  as valor_total_com_perfil_ano
    from fct
    group by ano_final
),
cotas as (
    select ano_final, 'negra' as grupo, 0.25 as meta_minima,
           sum(valor_pago_num) filter (where flag_negra)                 as valor_grupo,
           count(distinct identificador_unico) filter (where flag_negra) as qtd_agentes_grupo
    from fct group by ano_final
    union all
    select ano_final, 'indigena', 0.10,
           sum(valor_pago_num) filter (where flag_indigena),
           count(distinct identificador_unico) filter (where flag_indigena)
    from fct group by ano_final
    union all
    select ano_final, 'pcd', 0.05,
           sum(valor_pago_num) filter (where flag_pcd),
           count(distinct identificador_unico) filter (where flag_pcd)
    from fct group by ano_final
    {%- if incluir_territorio %}
    union all
    -- 4ª cota: território vulnerabilizado (periférico), meta 20%. Só LPG.
    select ano_final, 'territorio_vulneravel', 0.20,
           sum(valor_pago_num) filter (where flag_territorio_vulneravel),
           count(distinct identificador_unico) filter (where flag_territorio_vulneravel)
    from fct group by ano_final
    {%- endif %}
)
select
    '{{ programa }}'                                                                     as programa,
    c.ano_final,
    c.grupo,
    coalesce(c.valor_grupo, 0)                                                           as valor_grupo,
    t.valor_total_ano,
    t.valor_total_com_perfil_ano,
    round(coalesce(c.valor_grupo, 0) / nullif(t.valor_total_ano, 0) * 100, 2)            as pct_sobre_total,
    round(coalesce(c.valor_grupo, 0) / nullif(t.valor_total_com_perfil_ano, 0) * 100, 2) as pct_sobre_com_perfil,
    c.qtd_agentes_grupo,
    round(c.meta_minima * 100, 0)                                                        as meta_minima_pct,
    case
        when coalesce(c.valor_grupo, 0) / nullif(t.valor_total_com_perfil_ano, 0) >= c.meta_minima
        then 'alcancada' else 'descumprida'
    end as status_sobre_com_perfil
from cotas c
join totais t using (ano_final)
order by c.ano_final, c.grupo
{% endmacro %}
