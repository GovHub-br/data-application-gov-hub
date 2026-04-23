{{ config(materialized="table") }}

with
    coordenacoes as (
        select
            {{ safe_bigint('coordenacaoid') }} as coordenacaoid,
            coordenacaonome::text as coordenacaonome,
            coordenacaosigla::text as coordenacaosigla,
            coordenacaodescricao::text as coordenacaodescricao,
            {{ safe_bigint('coordenadorid') }} as coordenadorid,
            {{ safe_bigint('diretoriaid') }} as diretoriaid,
            {{ safe_boolean('ativa') }} as ativa,
            codigosiape::text as codigosiape
        from {{ source("ipea_pro", "coordenacoes") }}
    )

select *
from coordenacoes
