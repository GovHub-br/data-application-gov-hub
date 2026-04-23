{{ config(materialized="table") }}

with
    registrofinanceiroemprojetos as (
        select
            {{ safe_bigint('registrofinanceiroid') }} as registrofinanceiroid,
            descricaoinsumo::text as descricaoinsumo,
            {{ safe_numeric('valorunitarioinsumo') }} as valorunitarioinsumo,
            {{ safe_bigint('grupoentidadeid') }} as grupoentidadeid,
            {{ safe_bigint('insumofinanceiroid') }} as insumofinanceiroid,
            {{ safe_bigint('projetoid') }} as projetoid,
            {{ safe_bigint('quantidadeinsumo') }} as quantidadeinsumo,
            descricaofonte::text as descricaofonte
        from {{ source("ipea_pro", "registrofinanceiroemprojetos") }}
    )

select *
from registrofinanceiroemprojetos
