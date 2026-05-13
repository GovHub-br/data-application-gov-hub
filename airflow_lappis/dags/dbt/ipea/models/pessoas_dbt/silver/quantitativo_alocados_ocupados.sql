with
    siape_sem_duplicatas as (select distinct * from {{ ref("dados_funcionais") }}),
    siorg_sem_duplicatas as (
        select distinct * from {{ ref("estrutura_organizacional_cargos") }}
    ),

    codigos_siorg as (
        select funcao, nomeunidade, siglaunidade, denominacao, count(*) as qtd_vagas_cargo, max(dt_ingest) as dt_ingest
        from siorg_sem_duplicatas
        group by funcao, nomeunidade, siglaunidade, denominacao
    ),

    codigos_siape as (
        select
            cod_funcao,
            nome_uorg_exercicio,
            sigla_uorg_exercicio,
            nome_cargo,
            count(*) as qtd_vagas_ocupadas,
            max(dt_ingest) as dt_ingest
        from siape_sem_duplicatas
        where cod_funcao is not null and dt_ocorr_aposentadoria is null
        group by cod_funcao, nome_uorg_exercicio, sigla_uorg_exercicio, nome_cargo
    ),

    codigo_siorg_combinado as (
        select
            replace(funcao, ' ', '') as funcao,
            nomeunidade,
            case
                when siglaunidade = 'GABIN-IPEA' then 'GABIN' else siglaunidade
            end as siglaunidade,
            denominacao,
            substring(replace(funcao, ' ', ''), 1, 1) || substring(
                replace(funcao, ' ', ''), length(replace(funcao, ' ', '')) - 2, 3
            ) as codigo_combinacao_siorg,
            qtd_vagas_cargo,
            dt_ingest
        from codigos_siorg
    ),

    codigo_siape_combinado as (
        select
            cod_funcao,
            nome_uorg_exercicio,
            sigla_uorg_exercicio,
            nome_cargo,
            substring(cod_funcao, 1, 1) || substring(
                cod_funcao, length(cod_funcao) - 2, 3
            ) as codigo_combinacao_siape,
            qtd_vagas_ocupadas,
            dt_ingest
        from codigos_siape
    ),

    primeira_correlacao as (
        select
            siorg.funcao,
            siorg.nomeunidade,
            siorg.siglaunidade,
            siorg.denominacao,
            siorg.codigo_combinacao_siorg,
            siorg.qtd_vagas_cargo,
            siape.cod_funcao,
            siape.nome_uorg_exercicio,
            siape.sigla_uorg_exercicio,
            siape.nome_cargo,
            siape.codigo_combinacao_siape,
            siape.qtd_vagas_ocupadas,
            greatest(siorg.dt_ingest, siape.dt_ingest) as dt_ingest,
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
            codigo_siape_combinado as siape
            on siorg.codigo_combinacao_siorg = siape.codigo_combinacao_siape
            and siorg.siglaunidade = siape.sigla_uorg_exercicio
    )

select
    cod_funcao as codigo_siape,
    funcao as codigo_siorg,
    coalesce(nomeunidade, nome_uorg_exercicio) as nomeunidade,
    coalesce(siglaunidade, sigla_uorg_exercicio) as siglaunidade,
    coalesce(denominacao, nome_cargo) as nome_cargo,
    qtd_vagas_cargo,
    coalesce(qtd_vagas_ocupadas, 0) as qtd_vagas_ocupadas,
    case
        when qtd_vagas_cargo is null
        then null
        when qtd_vagas_ocupadas is null
        then qtd_vagas_cargo
        else (qtd_vagas_cargo - qtd_vagas_ocupadas)
    end as qtd_cargos_vagos,
    dt_ingest
from primeira_correlacao
