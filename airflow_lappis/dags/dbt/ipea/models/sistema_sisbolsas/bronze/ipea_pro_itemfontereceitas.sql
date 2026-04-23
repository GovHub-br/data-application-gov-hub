{{ config(materialized="table") }}

with
    itemfontereceitas as (
        select
            {{ safe_bigint('itemfontereceitaid') }} as itemfontereceitaid,
            nomeitemfontereceita::text as nomeitemfontereceita,
            descricaoitemfontereceita::text as descricaoitemfontereceita,
            observacao::text as observacao
        from {{ source("ipea_pro", "itemfontereceitas") }}
    )

select *
from itemfontereceitas
