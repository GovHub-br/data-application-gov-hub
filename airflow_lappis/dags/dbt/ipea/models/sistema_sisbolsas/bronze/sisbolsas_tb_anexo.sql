{{ config(materialized="table") }}

with
    sisbolsas_tb_anexo as (
        select
            co_anexo::text as co_anexo,
            ds_anexo::text as ds_anexo,
            co_tipo_anexo::text as co_tipo_anexo,
            ds_original::text as ds_original,
            dt_criacao::text as dt_criacao
        from {{ source("sisbolsas", "tb_anexo") }}
    )

select *
from sisbolsas_tb_anexo
