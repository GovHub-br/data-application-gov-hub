{{ config(materialized="table") }}

with
    sisbolsas_tb_chapubli_diretoria as (
        select
            co_chamada_publica::text as co_chamada_publica,
            co_diretoria::text as co_diretoria
        from {{ source("sisbolsas", "tb_chapubli_diretoria") }}
    )

select *
from sisbolsas_tb_chapubli_diretoria
