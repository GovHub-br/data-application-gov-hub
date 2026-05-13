{{ config(materialized="table") }}

with
    sisbolsas_tb_estado as (
        select
            co_estado::text as co_estado,
            ds_uf::text as ds_uf,
            ds_nome::text as ds_nome
        from {{ source("sisbolsas", "tb_estado") }}
    )

select *
from sisbolsas_tb_estado
