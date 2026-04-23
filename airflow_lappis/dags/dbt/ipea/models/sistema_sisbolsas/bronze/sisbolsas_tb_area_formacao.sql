{{ config(materialized="table") }}

with
    sisbolsas_tb_area_formacao as (
        select
            co_area_formacao::text as co_area_formacao,
            ds_area_formacao::text as ds_area_formacao
        from {{ source("sisbolsas", "tb_area_formacao") }}
    )

select *
from sisbolsas_tb_area_formacao
