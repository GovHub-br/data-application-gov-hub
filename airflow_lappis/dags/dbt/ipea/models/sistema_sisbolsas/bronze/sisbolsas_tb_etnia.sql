{{ config(materialized="table") }}

with
    sisbolsas_tb_etnia as (
        select co_etnia::text as co_etnia, ds_etnia::text as ds_etnia
        from {{ source("sisbolsas", "tb_etnia") }}
    )

select *
from sisbolsas_tb_etnia
