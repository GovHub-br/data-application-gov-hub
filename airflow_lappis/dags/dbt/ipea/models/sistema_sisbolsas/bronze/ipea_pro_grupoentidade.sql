{{ config(materialized="table") }}

with
    grupoentidade as (
        select
            {{ safe_bigint('grupoentidadeid') }} as grupoentidadeid,
            nome::text as nome
        from {{ source("ipea_pro", "grupoentidade") }}
    )

select *
from grupoentidade
