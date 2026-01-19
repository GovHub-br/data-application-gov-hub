with
    preparacao as (
        select
            du.codigo_orgao::integer as codigo_orgao,
            du.codigo_orgao_uorg as combinacao_codigo_lista,
            (right(du.codigo_orgao_uorg, 7))::integer as codigo_lista_uorg,
            du.sigla_uorg as sigla_uorg,
            max(du.dt_ingest) as dt_ingest_du
        from {{ ref("dados_uorg") }} du
        group by 1, 2, 3, 4
    ),

    join_lista_uorgo_dados_uorg as (
        select
            p.codigo_orgao,
            p.combinacao_codigo_lista,
            p.codigo_lista_uorg,
            p.sigla_uorg,
            p.dt_ingest_du,
            lu.dt_ultima_transacao,
            lu.nome as nome_unidade,
            max(lu.dt_ingest) as dt_ingest_lu
        from preparacao p
        join {{ ref("lista_uorgs") }} lu on p.codigo_lista_uorg = lu.codigo
        group by 1, 2, 3, 4, 5, 6, 7
    ),

    unidade_organizacional as (
        select distinct
            *, case when sigla = 'GABIN-IPEA' then 'GABIN' else sigla end as sigla_unidade
        from {{ ref("unidade_organizacional") }}
    ),

    tabela_corralacao_uorgs as (
        select
            coalesce(a.nome_unidade, uo.nome) as nome_unidade,
            coalesce(a.sigla_uorg, sigla_unidade) as sigla_uorg,
            a.codigo_lista_uorg as codigo_unidade_siape,
            uo.codigounidade as codigo_unidade_siorg,
            greatest(a.dt_ingest_du, a.dt_ingest_lu) as dt_ingest,
            case
                when a.nome_unidade is null and uo.nome is not null
                then 'apenas_siorg'
                when a.nome_unidade is not null and uo.nome is null
                then 'apenas_siape'
                when a.nome_unidade is not null and uo.nome is not null
                then 'ambos'
            end as tipo_correlacao
        from join_lista_uorgo_dados_uorg a
        full join unidade_organizacional uo on a.sigla_uorg = uo.sigla_unidade
    )

select *
from tabela_corralacao_uorgs
