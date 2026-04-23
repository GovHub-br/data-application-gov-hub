{{ config(materialized="table") }}

with
    sisbolsas_tb_chapubli_unidade as (
        select
            co_chamada_publica::text as co_chamada_publica,
            co_unidade::text as co_unidade
        from {{ source("sisbolsas", "tb_chapubli_unidade") }}
    )

select *
from sisbolsas_tb_chapubli_unidade
