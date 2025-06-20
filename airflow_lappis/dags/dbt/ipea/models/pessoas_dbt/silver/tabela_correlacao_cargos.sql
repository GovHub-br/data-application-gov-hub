with
    codigos_siorg as (
        select distinct
            replace(funcao, ' ', '') as funcao,
            nomeunidade,
            case
                when siglaunidade = 'GABIN-IPEA' then 'GABIN' else siglaunidade
            end as siglaunidade,
            denominacao
        from {{ ref("estrutura_organizacional_cargos") }}
    ),

    codigos_siape as (
        select distinct
            cod_funcao,
            nome_uorg_exercicio,
            sigla_uorg_exercicio,
            nome_cargo,
            matricula_siape,
            substring(cod_funcao, 1, 1)
            || substring(cod_funcao, length(cod_funcao) - 2, 3) as codigo_combinacao_siape
        from {{ ref("dados_funcionais") }}
        where cod_funcao is not null and dt_ocorr_aposentadoria is null
    ),

    codigo_siorg_combinado as (
        select
            *,
            substring(funcao, 1, 1)
            || substring(funcao, length(funcao) - 2, 3) as codigo_combinacao_siorg
        from codigos_siorg
    ),

    primeira_correlacao as (
        select
            *,
            case
                when
                    siorg.codigo_combinacao_siorg is not null
                    and siape.codigo_combinacao_siape is not null
                then 'inner'
                when
                    siorg.codigo_combinacao_siorg is not null
                    and siape.codigo_combinacao_siape is null
                then 'left'
                when
                    siorg.codigo_combinacao_siorg is null
                    and siape.codigo_combinacao_siape is not null
                then 'right'
            end as tipo_correlacao
        from codigo_siorg_combinado as siorg
        full join
            codigos_siape as siape
            on siorg.codigo_combinacao_siorg = siape.codigo_combinacao_siape
            and siorg.siglaunidade = siape.sigla_uorg_exercicio
    ),

    tabela_correlacao_cargos as (
        select
            cod_funcao as codigo_siape,
            funcao as codigo_siorg,
            coalesce(nomeunidade, nome_uorg_exercicio) as nomeunidade,
            coalesce(siglaunidade, sigla_uorg_exercicio) as siglaunidade,
            coalesce(denominacao, nome_cargo) as nome_cargo,
            matricula_siape
        from primeira_correlacao
    )

select *
from tabela_correlacao_cargos
