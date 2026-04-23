{{ config(materialized="table") }}

with
    sisbolsas_tb_fonte_financeira as (
        select
            co_fonte_financeira::text as co_fonte_financeira,
            ds_fonte_financeira::text as ds_fonte_financeira
        from {{ source("sisbolsas", "tb_fonte_financeira") }}
    )

select *
from sisbolsas_tb_fonte_financeira
