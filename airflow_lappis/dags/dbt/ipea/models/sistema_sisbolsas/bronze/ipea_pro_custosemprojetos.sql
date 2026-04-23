{{ config(materialized="table") }}

with
    custosemprojetos as (
        select
            {{ safe_bigint('custoemprojetoid') }} as custoemprojetoid,
            {{ safe_bigint('itemcustoid') }} as itemcustoid,
            {{ safe_bigint('quantidadeitemcusto') }} as quantidadeitemcusto,
            {{ safe_bigint('projetoid') }} as projetoid,
            descricaoitemcusto::text as descricaoitemcusto,
            {{ safe_date('datainicial') }} as datainicial,
            {{ safe_date('datafinal') }} as datafinal,
            {{ safe_numeric('custoespecifico') }} as custoespecifico
        from {{ source("ipea_pro", "custosemprojetos") }}
    )

select *
from custosemprojetos
