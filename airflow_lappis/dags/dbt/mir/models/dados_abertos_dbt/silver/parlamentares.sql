{{ config(materialized="table") }}

with
    bronze_deputados as (select * from {{ ref("deputados") }}),

    bronze_senadores as (select * from {{ ref("senadores") }}),

    sigla_map as (
        select
            trim(upper(sigla_origem)) as sigla_origem,
            trim(upper(sigla_canonica)) as sigla_canonica
        from {{ ref("partidos_map") }}
    ),

    parlamentares_unificados as (
        select
            id as id_parlamentar,
            trim(upper(nome)) as chave_join_nome,
            nome as nome_parlamentar,
            'Deputado' as cargo_parlamentar,
            siglapartido as sigla_partido,
            siglauf as uf_parlamentar,
            urlfoto as url_foto,
            email
        from bronze_deputados

        union all

        select
            id as id_parlamentar,
            trim(upper(nome_parlamentar)) as chave_join_nome,
            nome_parlamentar as nome_parlamentar,
            'Senador' as cargo_parlamentar,
            sigla_partido as sigla_partido,
            uf as uf_parlamentar,
            url_foto as url_foto,
            email
        from bronze_senadores
    ),

    parlamentares_padronizados as (
        select
            p.*, coalesce(m.sigla_canonica, p.sigla_partido) as sigla_partido_padronizada
        from parlamentares_unificados p
        left join sigla_map m on trim(upper(p.sigla_partido)) = m.sigla_origem
    ),

    partidos_logo as (
        select trim(upper(sigla)) as chave_join_sigla_partido, logo_url
        from {{ ref("partidos_logo") }}
    )

select
    p.id_parlamentar,
    p.chave_join_nome,
    p.nome_parlamentar,
    p.cargo_parlamentar,

    p.sigla_partido_padronizada as sigla_partido,

    p.uf_parlamentar,
    p.url_foto,
    p.email,
    pl.logo_url as url_logo_partido

from parlamentares_padronizados p

left join
    partidos_logo pl
    on trim(upper(p.sigla_partido_padronizada)) = pl.chave_join_sigla_partido
