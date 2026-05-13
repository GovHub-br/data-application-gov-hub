{{ config(materialized="table") }}

with
    ingresso_contrapartida_raw as (
        select
            nr_convenio::integer as nr_convenio,
            to_date(nullif(dt_ingresso_contrapartida, ''), 'DD/MM/YYYY') as dt_ingresso_contrapartida,
            replace(nullif(vl_ingresso_contrapartida, ''), ',', '.')::numeric(15, 2) as vl_ingresso_contrapartida
        from {{ source("siconv", "ingresso_contrapartida") }}
    )

select *
from ingresso_contrapartida_raw