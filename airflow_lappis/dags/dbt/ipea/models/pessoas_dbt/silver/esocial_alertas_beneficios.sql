-- Alertas de pré-validação eSocial sobre pensões. Regras R11 e R12 (pensão alimentícia,
-- fechamento do IRRF na S-1200), R13 (CPF de pensionista zerado ou repetido) e R14
-- (instituidor de pensão sem pensionista, S-2410). Catálogo: seed esocial_regras.
-- Na R13, os dependentes só entram depois que a DAG de dados_dependentes passar a gravar
-- cpf_dependente: antes disso todos apareceriam sem CPF.
-- O SIAPE traz o CPF de quem recebe (beneficiário) e só o nome do alimentado. Quando os
-- dois nomes diferem, quem recebe é o representante legal e falta o CPF do alimentado,
-- que o eSocial exige.
-- Granularidade: servidor (ou instituidor) x beneficiário x regra
with
    pensoes as (
        select
            cpf_servidor as cpf,
            matricula_servidor as matricula_siape,
            cpf_beneficiario,
            nome_beneficiario,
            nome_alimentado,
            conta_beneficiario,
            dt_ingest
        from {{ ref("dados_pa") }}
    ),

    uorg_servidor as (
        select distinct on (cpf) cpf, matricula_siape, sigla_uorg_exercicio
        from {{ ref("dados_funcionais") }}
        -- Sem filtrar exclusão: o instituidor da pensão pode ter o vínculo encerrado
        order by cpf, dt_ocorr_exclusao nulls first, sigla_uorg_exercicio nulls last
    ),

    pensoes_uorg as (
        select p.*, u.sigla_uorg_exercicio
        from pensoes p
        left join uorg_servidor u on p.cpf = u.cpf
    ),

    -- Alimentado diferente de quem recebe: o recebedor é o representante legal
    recebe_representante as (
        select *
        from pensoes_uorg
        where
            nome_alimentado is not null
            and upper(trim(nome_alimentado))
            <> upper(trim(coalesce(nome_beneficiario, '')))
    ),

    -- R11: falta o CPF do alimentado (beneficiário da decisão judicial)
    r11 as (
        select
            'R11' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'cpf_beneficiario' as campo,
            'CPF do beneficiário vazio ou inválido' as valor_encontrado,
            dt_ingest
        from pensoes_uorg
        where not {{ esocial_cpf_valido("cpf_beneficiario") }}

        union all

        select
            'R11',
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'nome_alimentado',
            'alimentado diferente do beneficiário: falta o CPF do alimentado',
            dt_ingest
        from recebe_representante
    ),

    -- R12: conta da pensão em nome do representante, não do alimentado
    r12 as (
        select
            'R12' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'conta_beneficiario' as campo,
            'conta em nome do representante legal' as valor_encontrado,
            dt_ingest
        from recebe_representante
        where conta_beneficiario is not null
    ),

    -- Pensão por morte: pensionistas de cada instituidor
    pensionistas as (
        select
            p.cpf_instituidor as cpf,
            u.matricula_siape,
            u.sigla_uorg_exercicio,
            p.cpf_pensionista,
            p.nome_pensionista,
            p.matricula_pensionista,
            p.dt_ingest
        from {{ ref("pensoes_instituidas") }} p
        left join uorg_servidor u on p.cpf_instituidor = u.cpf
    ),

    dependentes as (
        select
            d.cpf,
            coalesce(d.matricula, u.matricula_siape) as matricula_siape,
            u.sigla_uorg_exercicio,
            d.cpf_dependente,
            d.nome_dependente,
            d.dt_ingest
        from {{ ref("dados_dependentes") }} d
        left join uorg_servidor u on d.cpf = u.cpf
        where
            exists (
                select 1
                from {{ ref("dados_dependentes") }}
                where cpf_dependente is not null
            )
    ),

    cpf_dependente_repetido as (
        select cpf_dependente
        from dependentes
        where cpf_dependente is not null
        group by cpf_dependente
        having count(distinct upper(trim(nome_dependente))) > 1
    ),

    -- Mesmo CPF com nomes diferentes: um dos cadastros está com o CPF errado
    cpf_pensionista_repetido as (
        select cpf_pensionista
        from pensionistas
        where cpf_pensionista is not null
        group by cpf_pensionista
        having count(distinct upper(trim(nome_pensionista))) > 1
    ),

    cpf_alimentado_repetido as (
        select cpf_beneficiario
        from pensoes_uorg
        where cpf_beneficiario is not null
        group by cpf_beneficiario
        having count(distinct upper(trim(nome_beneficiario))) > 1
    ),

    -- R13: CPF de pensionista zerado (ou inválido) ou repetido em pessoas diferentes.
    -- O CPF inválido do beneficiário da pensão alimentícia já é alertado pela R11.
    r13 as (
        select
            'R13' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'cpf_pensionista' as campo,
            'pensionista '
            || coalesce(matricula_pensionista, nome_pensionista, 'sem identificação')
            || ': CPF vazio, zerado ou inválido' as valor_encontrado,
            dt_ingest
        from pensionistas
        where not {{ esocial_cpf_valido("cpf_pensionista") }}

        union all

        select
            'R13',
            pe.cpf,
            pe.matricula_siape,
            pe.sigla_uorg_exercicio,
            'cpf_pensionista',
            'pensionista ' || coalesce(
                pe.matricula_pensionista, pe.nome_pensionista, 'sem identificação'
            )
            || ': CPF usado por mais de uma pessoa',
            pe.dt_ingest
        from pensionistas pe
        inner join cpf_pensionista_repetido r on pe.cpf_pensionista = r.cpf_pensionista

        union all

        select
            'R13',
            pa.cpf,
            pa.matricula_siape,
            pa.sigla_uorg_exercicio,
            'cpf_beneficiario',
            'beneficiário da pensão alimentícia: CPF usado por mais de uma pessoa',
            pa.dt_ingest
        from pensoes_uorg pa
        inner join cpf_alimentado_repetido r on pa.cpf_beneficiario = r.cpf_beneficiario

        union all

        select
            'R13',
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'cpf_dependente',
            'dependente '
            || coalesce(nome_dependente, 'sem nome')
            || ': CPF vazio, zerado ou inválido',
            dt_ingest
        from dependentes
        where not {{ esocial_cpf_valido("cpf_dependente") }}

        union all

        select
            'R13',
            de.cpf,
            de.matricula_siape,
            de.sigla_uorg_exercicio,
            'cpf_dependente',
            'dependente '
            || coalesce(de.nome_dependente, 'sem nome')
            || ': CPF usado por mais de uma pessoa',
            de.dt_ingest
        from dependentes de
        inner join cpf_dependente_repetido r on de.cpf_dependente = r.cpf_dependente
    ),

    -- R14: instituidor de pensão (situação 15) sem nenhum pensionista identificado
    instituidores as (
        select distinct on (cpf) cpf, matricula_siape, sigla_uorg_exercicio, dt_ingest
        from {{ ref("dados_funcionais") }}
        where
            cod_situacao_funcional = '15'
            or nome_situacao_funcional = 'INSTITUIDOR PENSAO'
        order by cpf, dt_ingest desc
    ),

    instituidores_com_pensionista as (
        select distinct cpf
        from pensionistas
        where cpf_pensionista is not null or nome_pensionista is not null
    ),

    r14 as (
        select
            'R14' as regra_id,
            i.cpf,
            i.matricula_siape,
            i.sigla_uorg_exercicio,
            'cod_situacao_funcional' as campo,
            'instituidor de pensão sem pensionista em pensoes_instituidas'
            as valor_encontrado,
            i.dt_ingest
        from instituidores i
        left join instituidores_com_pensionista c on i.cpf = c.cpf
        where c.cpf is null
    )

select *
from r11
union all
select *
from r12
union all
select *
from r13
union all
select *
from r14
