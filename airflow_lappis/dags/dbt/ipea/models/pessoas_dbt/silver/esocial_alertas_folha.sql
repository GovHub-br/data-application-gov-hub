-- Alertas de pré-validação eSocial sobre a folha (S-1200), na competência de pagamento
-- mais recente de dados_financeiros. Regra R18. Catálogo: seed esocial_regras.
-- A R17 (rubricas de IRRF e CPSS) não roda: a ingestão traz só as rubricas genéricas
-- 0000 e 9999 (Líquido), sem o detalhe por rubrica.
-- Granularidade: servidor pago com alerta bloqueante de cadastro
with
    competencia as (
        select max(mes_ano_pagamento) as mes_ano_pagamento
        from {{ ref("dados_financeiros") }}
    ),

    folha as (
        select f.*
        from {{ ref("dados_financeiros") }} f
        inner join competencia c on f.mes_ano_pagamento = c.mes_ano_pagamento
    ),

    -- R18: servidor na folha com alerta bloqueante de cadastro (tende ao erro 162)
    bloqueantes_cadastro as (
        select distinct a.cpf, a.matricula_siape, a.sigla_uorg_exercicio, a.regra_id
        from {{ ref("esocial_alertas_cadastro") }} a
        inner join {{ ref("esocial_regras") }} r on a.regra_id = r.regra_id
        where r.severidade = 'Bloqueante' and r.evento in ('S-2200', 'S-2300')
    ),

    pagos as (
        select cpf, max(dt_ingest) as dt_ingest
        from folha
        where cpf is not null
        group by cpf
    ),

    r18 as (
        select
            'R18' as regra_id,
            b.cpf,
            b.matricula_siape,
            b.sigla_uorg_exercicio,
            'cpf' as campo,
            'pago na competência com alertas bloqueantes: ' || string_agg(
                distinct b.regra_id, ', ' order by b.regra_id
            ) as valor_encontrado,
            max(p.dt_ingest) as dt_ingest
        from bloqueantes_cadastro b
        inner join pagos p on b.cpf = p.cpf
        group by b.cpf, b.matricula_siape, b.sigla_uorg_exercicio
    )

select *
from r18
