{{ config(materialized="table") }}

with
    fontesreceitas as (
        select
            {{ safe_bigint('fontereceitaid') }} as fontereceitaid,
            {{ safe_bigint('projetoid') }} as projetoid,
            {{ safe_bigint('itemfontereceitaid') }} as itemfontereceitaid,
            descricaofonte::text as descricaofonte,
            {{ safe_numeric('valortotalfonte') }} as valortotalfonte,
            {{ safe_date('datainiciofonte') }} as datainiciofonte,
            {{ safe_date('datafinalfonte') }} as datafinalfonte,
            {{ safe_numeric('valortotalfonte_bkp') }} as valortotalfonte_bkp
        from {{ source("ipea_pro", "fontesreceitas") }}
    )

select *
from fontesreceitas
