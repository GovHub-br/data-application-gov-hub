{{ config(materialized="table") }}

with
    sisbolsas_tb_programa as (
        select
            co_programa::text as co_programa,
            ds_programa::text as ds_programa,
            co_programa_pai::text as co_programa_pai
        from {{ source("sisbolsas", "tb_programa") }}
    )

select *
from sisbolsas_tb_programa
