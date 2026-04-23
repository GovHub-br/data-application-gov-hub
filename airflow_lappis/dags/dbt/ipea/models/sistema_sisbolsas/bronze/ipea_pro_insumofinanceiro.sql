{{ config(materialized="table") }}

with
    insumofinanceiro as (
        select
            {{ safe_bigint('insumofinanceiroid') }} as insumofinanceiroid,
            item::text as item,
            unidade::text as unidade,
            descricao::text as descricao,
            {{ safe_boolean('possuivalorunitario') }} as possuivalorunitario,
            {{ safe_boolean('possuiquantidade') }} as possuiquantidade
        from {{ source("ipea_pro", "insumofinanceiro") }}
    )

select *
from insumofinanceiro
