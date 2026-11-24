-- Alertas de pré-validação eSocial sobre o cadastro de servidores (S-2200, S-2300,
-- S-2400, S-2410). Regras R01 a R10 e R16, exceto R04 (o SIAPE ingerido não traz o
-- órgão de destino da cessão) e R06 (o SIAPE não traz o CEP do servidor).
-- Catálogo das regras: seed esocial_regras.
-- Granularidade: vínculo (cpf + matricula_siape) x regra x campo
-- Códigos de situação funcional no SIAPE do IPEA (confirmados pela equipe):
-- 01 ativo permanente, 02 aposentado, 03 cedido/requisitado (requisitado de outro
-- órgão), 04 nomeado cargo comissionado, 08 ativo em outro órgão (cedido pelo IPEA),
-- 15 instituidor de pensão, 18 exercício descentralizado de carreira (requisitado),
-- 44 exercício art. 93 da Lei 8.112 (requisitado), 70 estagiário SIGEPE.
with
    vinculos as (
        select
            df.cpf,
            df.matricula_siape,
            df.sigla_uorg_exercicio,
            df.cod_situacao_funcional,
            df.nome_situacao_funcional,
            df.nome_regime_juridico,
            df.cod_orgao_origem,
            df.sigla_orgao_origem,
            df.cod_ocorr_aposentadoria,
            df.nome_ocorr_aposentadoria,
            df.dt_ocorr_aposentadoria,
            df.dt_ocorr_ingresso_orgao,
            df.dt_exercicio_no_orgao,
            dp.nome_pessoa,
            dp.dt_nascimento,
            dp.nome_nacionalidade,
            dp.nome_municipio_nascimento,
            dp.uf_nascimento,
            dp.cod_deficiencia_fisica,
            greatest(df.dt_ingest, dp.dt_ingest) as dt_ingest
        from {{ ref("dados_funcionais") }} df
        left join {{ ref("dados_pessoais") }} dp on df.cpf = dp.cpf
        where df.dt_ocorr_exclusao is null
    ),

    -- Vínculos em atividade: tudo que não é aposentadoria nem pensão
    vinculos_ativos as (
        select *
        from vinculos
        where
            coalesce(cod_situacao_funcional, '') not in ('02', '15')
            and coalesce(nome_situacao_funcional, '')
            not in ('APOSENTADO', 'INSTITUIDOR PENSAO')
    ),

    -- R01: qualificação cadastral (checagem formal de CPF, nome e nascimento)
    r01 as (
        select
            'R01' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'cpf' as campo,
            case
                when cpf is null then 'CPF vazio' else 'CPF inválido (dígito verificador)'
            end as valor_encontrado,
            dt_ingest
        from vinculos
        where not {{ esocial_cpf_valido("cpf") }}

        union all

        select
            'R01',
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'nome_pessoa',
            case
                when nome_pessoa is null
                then 'nome vazio'
                when nome_pessoa ~ '[0-9]'
                then 'nome com número'
                when nome_pessoa ~ '\m[A-Za-z]\.'
                then 'nome abreviado'
                else 'nome com caractere especial'
            end,
            dt_ingest
        from vinculos
        where
            nome_pessoa is null
            or nome_pessoa ~ '[0-9]'
            or nome_pessoa ~ '\m[A-Za-z]\.'
            or nome_pessoa ~ '[^[:alpha:] ''-]'

        union all

        select
            'R01',
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'dt_nascimento',
            case
                when dt_nascimento is null
                then 'data de nascimento vazia'
                else 'data de nascimento fora de faixa'
            end,
            dt_ingest
        from vinculos
        where
            dt_nascimento is null
            or dt_nascimento < date '1900-01-01'
            or dt_nascimento > current_date - interval '14 years'
    ),

    -- R02: regime jurídico incompatível com a situação funcional (codCateg)
    r02 as (
        select
            'R02' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'nome_regime_juridico' as campo,
            coalesce(nome_regime_juridico, 'regime vazio')
            || ' x '
            || coalesce(nome_situacao_funcional, 'situação vazia') as valor_encontrado,
            dt_ingest
        from vinculos
        where
            (
                (
                    cod_situacao_funcional = '70'
                    or nome_situacao_funcional = 'ESTAGIARIO SIGEPE'
                )
                and coalesce(nome_regime_juridico, '') <> 'CONVENIO/CONTRATO ESTAGIO'
            )
            or (
                nome_regime_juridico = 'CONVENIO/CONTRATO ESTAGIO'
                and coalesce(cod_situacao_funcional, '') <> '70'
                and coalesce(nome_situacao_funcional, '') <> 'ESTAGIARIO SIGEPE'
            )
    ),

    -- R03: matrícula em mais de um CPF, ou CPF com mais de um vínculo ativo
    matriculas_repetidas as (
        select matricula_siape
        from vinculos
        where matricula_siape is not null
        group by matricula_siape
        having count(distinct cpf) > 1
    ),

    cpfs_multiplos_vinculos as (
        select cpf, count(distinct matricula_siape) as qtd_vinculos
        from vinculos_ativos
        where cpf is not null
        group by cpf
        having count(distinct matricula_siape) > 1
    ),

    r03 as (
        select
            'R03' as regra_id,
            v.cpf,
            v.matricula_siape,
            v.sigla_uorg_exercicio,
            'matricula_siape' as campo,
            'matrícula usada por mais de um CPF' as valor_encontrado,
            v.dt_ingest
        from vinculos v
        inner join matriculas_repetidas mr on v.matricula_siape = mr.matricula_siape

        union all

        select
            'R03',
            v.cpf,
            v.matricula_siape,
            v.sigla_uorg_exercicio,
            'matricula_siape',
            c.qtd_vinculos || ' vínculos ativos no mesmo CPF',
            v.dt_ingest
        from vinculos_ativos v
        inner join cpfs_multiplos_vinculos c on v.cpf = c.cpf
    ),

    -- R05: requisitado de outro órgão (03, 18, 44) sem órgão de origem. No IPEA ele
    -- entra como trabalhador sem vínculo (S-2300, categoria 410), que pede os dados
    -- do órgão cedente.
    r05 as (
        select
            'R05' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'cod_orgao_origem' as campo,
            coalesce(nome_situacao_funcional, 'requisitado')
            || ' sem órgão de origem' as valor_encontrado,
            dt_ingest
        from vinculos
        where
            (
                cod_situacao_funcional in ('03', '18', '44')
                or nome_situacao_funcional
                in ('CEDIDO/REQUISITADO', 'EXERC DESCENT CARREI', 'EXERC. 7 ART93 8112')
            )
            and cod_orgao_origem is null
            and sigla_orgao_origem is null
    ),

    -- R07: município ou UF de nascimento ausente para brasileiros
    r07 as (
        select
            'R07' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            case
                when nome_municipio_nascimento is null
                then 'nome_municipio_nascimento'
                else 'uf_nascimento'
            end as campo,
            'vazio' as valor_encontrado,
            dt_ingest
        from vinculos
        where
            nome_nacionalidade ilike 'BRASILEIRO%'
            and (nome_municipio_nascimento is null or uf_nascimento is null)
    ),

    -- R08: endereço da unidade de exercício incompleto
    uorg_por_cpf as (
        select distinct
            on (cpf) cpf, logradouro_uorg, cep_uorg, codigo_municipio_uorg, uf_uorg
        from {{ ref("dados_uorg") }}
        order by cpf, dt_ingest desc
    ),

    r08 as (
        select
            'R08' as regra_id,
            v.cpf,
            v.matricula_siape,
            v.sigla_uorg_exercicio,
            case
                when u.logradouro_uorg is null
                then 'logradouro_uorg'
                when
                    coalesce(regexp_replace(u.cep_uorg, '[^0-9]', '', 'g'), '')
                    !~ '^[0-9]{8}$'
                then 'cep_uorg'
                when u.codigo_municipio_uorg is null
                then 'codigo_municipio_uorg'
                else 'uf_uorg'
            end as campo,
            'vazio ou inválido' as valor_encontrado,
            v.dt_ingest
        from vinculos_ativos v
        inner join uorg_por_cpf u on v.cpf = u.cpf
        where
            u.logradouro_uorg is null
            or coalesce(regexp_replace(u.cep_uorg, '[^0-9]', '', 'g'), '') !~ '^[0-9]{8}$'
            or u.codigo_municipio_uorg is null
            or u.uf_uorg is null
    ),

    aposentados as (
        select *
        from vinculos
        where cod_situacao_funcional = '02' or nome_situacao_funcional = 'APOSENTADO'
    ),

    -- R09: aposentadoria com data fora do dia 1º do mês
    r09 as (
        select
            'R09' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'dt_ocorr_aposentadoria' as campo,
            'aposentadoria no dia '
            || extract(day from dt_ocorr_aposentadoria)::int as valor_encontrado,
            dt_ingest
        from aposentados
        where
            dt_ocorr_aposentadoria is not null
            and extract(day from dt_ocorr_aposentadoria) <> 1
    ),

    -- R10: aposentadoria sem fundamentação (código ou descrição da ocorrência)
    r10 as (
        select
            'R10' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            case
                when cod_ocorr_aposentadoria is null
                then 'cod_ocorr_aposentadoria'
                else 'nome_ocorr_aposentadoria'
            end as campo,
            'vazio' as valor_encontrado,
            dt_ingest
        from aposentados
        where cod_ocorr_aposentadoria is null or nome_ocorr_aposentadoria is null
    ),

    -- R16: estágio no limite legal de 2 anos (Lei 11.788/2008, art. 11). O SIAPE não traz
    -- o término do estágio, então o prazo conta a partir do ingresso no órgão. Alerta
    -- quem já passou de 2 anos ou faltam 60 dias ou menos. Estagiário com deficiência
    -- fica de fora: para ele o limite de 2 anos não vale.
    estagiarios as (
        select
            *,
            coalesce(dt_ocorr_ingresso_orgao, dt_exercicio_no_orgao) as dt_inicio_estagio
        from vinculos_ativos
        where
            (
                cod_situacao_funcional = '70'
                or nome_situacao_funcional = 'ESTAGIARIO SIGEPE'
            )
            and cod_deficiencia_fisica is null
    ),

    r16 as (
        select
            'R16' as regra_id,
            cpf,
            matricula_siape,
            sigla_uorg_exercicio,
            'dt_ocorr_ingresso_orgao' as campo,
            case
                when dt_inicio_estagio is null
                then 'início do estágio vazio'
                when dt_inicio_estagio + interval '2 years' <= current_date
                then
                    'estágio passou de 2 anos (início em '
                    || to_char(dt_inicio_estagio, 'DD/MM/YYYY')
                    || ')'
                else
                    'estágio completa 2 anos em '
                    || to_char(dt_inicio_estagio + interval '2 years', 'DD/MM/YYYY')
            end as valor_encontrado,
            dt_ingest
        from estagiarios
        where
            dt_inicio_estagio is null
            or dt_inicio_estagio + interval '2 years' <= current_date + interval '60 days'
    )

select *
from r01
union all
select *
from r02
union all
select *
from r03
union all
select *
from r05
union all
select *
from r07
union all
select *
from r08
union all
select *
from r09
union all
select *
from r10
union all
select *
from r16
