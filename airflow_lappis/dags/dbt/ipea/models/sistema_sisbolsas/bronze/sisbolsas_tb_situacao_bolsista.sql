{{ config(materialized="table") }}

with
    sisbolsas_tb_situacao_bolsista as (
        select
            co_situacao_bolsista::text as co_situacao_bolsista,
            ds_situacao_bolsista::text as ds_situacao_bolsista
        from {{ source("sisbolsas", "tb_situacao_bolsista") }}
    )

select *
from sisbolsas_tb_situacao_bolsista
