-- Resumo da pré-validação eSocial por regra, incluindo regras sem alerta e as que
-- ainda não rodam por falta de dado.
-- Granularidade: regra
with
    contagem as (
        select
            regra_id,
            count(*) as qtd_alertas,
            count(distinct cpf) as qtd_cpfs,
            max(dt_ingest) as dt_ingest
        from {{ ref("esocial_alertas") }}
        group by regra_id
    )

select
    r.regra_id,
    r.regra,
    r.publico,
    r.evento,
    r.severidade,
    r.disponibilidade,
    coalesce(c.qtd_alertas, 0) as qtd_alertas,
    coalesce(c.qtd_cpfs, 0) as qtd_cpfs,
    c.dt_ingest
from {{ ref("esocial_regras") }} r
left join contagem c on r.regra_id = c.regra_id
