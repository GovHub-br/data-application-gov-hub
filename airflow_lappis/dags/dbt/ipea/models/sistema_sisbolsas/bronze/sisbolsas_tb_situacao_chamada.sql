{{ config(materialized="table") }}

with
    sisbolsas_tb_situacao_chamada as (
        select
            co_situacao_chamada::text as co_situacao_chamada,
            ds_situacao_chamada::text as ds_situacao_chamada
        from {{ source("sisbolsas", "tb_situacao_chamada") }}
    )

select *
from sisbolsas_tb_situacao_chamada
