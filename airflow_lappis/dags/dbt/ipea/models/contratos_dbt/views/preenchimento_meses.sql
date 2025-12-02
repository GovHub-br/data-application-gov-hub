-- Essa view será usada para preencher todos os gaps temporais
-- em tabelas gold com o propósito de eliminar decontinuidades 
-- nas visualizações em linha
with

    contractos_lista as (
        select contrato_id, min(mes_ref) as min_mes, max(mes_ref) as max_mes
        from {{ ref("cronogramas_faturas_mensal") }}
        group by contrato_id
    ),

    meses_lista as (select distinct mes_ref from {{ ref("cronogramas_faturas_mensal") }})

--
select c.contrato_id, m.mes_ref
from contractos_lista as c
left join meses_lista as m on (c.min_mes <= m.mes_ref) and (c.max_mes >= m.mes_ref)
