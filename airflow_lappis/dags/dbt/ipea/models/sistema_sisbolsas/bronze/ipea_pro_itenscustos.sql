{{ config(materialized="table") }}

with
    itenscustos as (
        select
            {{ safe_bigint('itemcustoid') }} as itemcustoid,
            nomeitem::text as nomeitem,
            descricaoitem::text as descricaoitem,
            unidadeitem::text as unidadeitem,
            {{ safe_numeric('valorunitarioitem') }} as valorunitarioitem,
            {{ safe_bigint('statusitemcadastradoid') }} as statusitemcadastradoid,
            {{ safe_bigint('categoriasitenscustoid') }} as categoriasitenscustoid,
            {{ safe_bigint('ordem') }} as ordem,
            {{ safe_boolean('ativo') }} as ativo,
            {{ safe_boolean('padrao') }} as padrao
        from {{ source("ipea_pro", "itenscustos") }}
    )

select *
from itenscustos
