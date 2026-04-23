{{ config(materialized="table") }}

with
    sisbolsas_tb_situacao_concessao as (
        select
            co_situacao_concessao::text as co_situacao_concessao,
            ds_situacao_concessao::text as ds_situacao_concessao
        from {{ source("sisbolsas", "tb_situacao_concessao") }}
    )

select *
from sisbolsas_tb_situacao_concessao
