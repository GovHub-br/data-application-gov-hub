{{ config(materialized="table") }}

with
    sisbolsas_tb_nivel_escolaridade as (
        select
            co_nivel_escolaridade::text as co_nivel_escolaridade,
            ds_nivel_escolaridade::text as ds_nivel_escolaridade
        from {{ source("sisbolsas", "tb_nivel_escolaridade") }}
    )

select *
from sisbolsas_tb_nivel_escolaridade
