{{ config(materialized="table") }}

with
    sisbolsas_tb_diretoria as (
        select
            co_diretoria::text as co_diretoria,
            ds_diretoria::text as ds_diretoria,
            ds_sigla::text as ds_sigla
        from {{ source("sisbolsas", "tb_diretoria") }}
    )

select *
from sisbolsas_tb_diretoria
