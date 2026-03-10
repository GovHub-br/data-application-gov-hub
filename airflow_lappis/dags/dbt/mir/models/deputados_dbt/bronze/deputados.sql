{{ config(materialized="table") }}

with
    deputados_raw as (
        select
            -- Conversão de tipos e formatação de colunas
            id::integer as id,
            nome::text as nome,
            siglapartido::text as siglapartido,
            siglauf::text as siglauf,
            idlegislatura::integer as idlegislatura,
            urlfoto::text as urlfoto,
            email::text as email,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("dados_abertos", "deputados") }}
    )

select *
from deputados_raw
