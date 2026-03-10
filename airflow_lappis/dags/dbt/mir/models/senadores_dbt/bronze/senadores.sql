{{ config(materialized="table") }}

with
    senadores_raw as (
        select
            id::integer as id,
            nome_parlamentar::text as nome_parlamentar,
            sexo::text as sexo,
            forma_tratamento::text as forma_tratamento,
            url_foto::text as url_foto,
            url_pagina::text as url_pagina,
            email::text as email,
            sigla_partido::text as sigla_partido,
            uf::text as uf,
            (dt_ingest || '-03:00')::timestamptz as dt_ingest
        from {{ source("senado_federal", "senadores") }}
    )

select *
from senadores_raw
