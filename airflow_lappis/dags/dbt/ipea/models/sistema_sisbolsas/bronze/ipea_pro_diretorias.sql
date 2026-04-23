{{ config(materialized="table") }}

with
    diretorias as (
        select
            {{ safe_bigint('diretoriaid') }} as diretoriaid,
            diretorianome::text as diretorianome,
            diretoriasigla::text as diretoriasigla,
            diretoriadescricao::text as diretoriadescricao,
            {{ safe_bigint('diretorid') }} as diretorid,
            {{ safe_bigint('diretoradjuntoid') }} as diretoradjuntoid,
            codigosiape::text as codigosiape
        from {{ source("ipea_pro", "diretorias") }}
    )

select *
from diretorias
