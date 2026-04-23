{{ config(materialized="table") }}

with
    sisbolsas_tb_chapubli_fontfina as (
        select
            co_chamada_publica::text as co_chamada_publica,
            co_fonte_financeira::text as co_fonte_financeira
        from {{ source("sisbolsas", "tb_chapubli_fontfina") }}
    )

select *
from sisbolsas_tb_chapubli_fontfina
