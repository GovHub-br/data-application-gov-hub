-- Fila de alertas de pré-validação dos dados de pessoas antes do envio ao eSocial.
-- Junta os alertas silver com o catálogo de regras (seed esocial_regras).
-- Granularidade: alerta (regra + cpf + matrícula + campo + valor)
with
    alertas as (
        select *
        from {{ ref("esocial_alertas_cadastro") }}
        union all
        select *
        from {{ ref("esocial_alertas_beneficios") }}
        union all
        select *
        from {{ ref("esocial_alertas_folha") }}
    ),

    -- Um vínculo repetido na fonte não deve gerar alerta duplicado
    alertas_unicos as (
        select
            regra_id,
            cpf,
            matricula_siape,
            campo,
            valor_encontrado,
            max(sigla_uorg_exercicio) as sigla_uorg_exercicio,
            max(dt_ingest) as dt_ingest
        from alertas
        group by regra_id, cpf, matricula_siape, campo, valor_encontrado
    ),

    nomes as (
        select distinct on (cpf) cpf, nome_pessoa
        from {{ ref("dados_pessoais") }}
        order by cpf, dt_ingest desc
    )

select
    md5(
        a.regra_id
        || '|'
        || coalesce(a.cpf, '')
        || '|'
        || coalesce(a.matricula_siape, '')
        || '|'
        || a.campo
        || '|'
        || a.valor_encontrado
    ) as alerta_id,
    a.regra_id,
    r.regra,
    r.publico,
    r.evento,
    r.erro_esocial,
    r.severidade,
    a.cpf,
    a.matricula_siape,
    n.nome_pessoa,
    a.sigla_uorg_exercicio,
    a.campo,
    a.valor_encontrado,
    a.dt_ingest
from alertas_unicos a
inner join {{ ref("esocial_regras") }} r on a.regra_id = r.regra_id
left join nomes n on a.cpf = n.cpf
