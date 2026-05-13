{{ config(materialized="table") }}

with
    statusprojetos as (
        select
            {{ safe_bigint('statusprojetoid') }} as statusprojetoid,
            nomestatus::text as nomestatus,
            descricaostatus::text as descricaostatus,
            {{ safe_boolean('ativo') }} as ativo
        from {{ source("ipea_pro", "statusprojetos") }}
    )

select *
from statusprojetos
