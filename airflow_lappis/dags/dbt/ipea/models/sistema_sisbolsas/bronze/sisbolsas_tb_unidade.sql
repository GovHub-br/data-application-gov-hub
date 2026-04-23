{{ config(materialized="table") }}

with
    sisbolsas_tb_unidade as (
        select
            co_unidade::text as co_unidade,
            ds_unidade::text as ds_unidade,
            co_estado::text as co_estado,
            ds_sigla::text as ds_sigla,
            co_diretoria::text as co_diretoria
        from {{ source("sisbolsas", "tb_unidade") }}
    )

select *
from sisbolsas_tb_unidade
