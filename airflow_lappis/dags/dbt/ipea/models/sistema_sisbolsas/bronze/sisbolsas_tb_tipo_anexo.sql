{{ config(materialized="table") }}

with
    sisbolsas_tb_tipo_anexo as (
        select
            co_tipo_anexo::text as co_tipo_anexo,
            ds_tipo_anexo::text as ds_tipo_anexo
        from {{ source("sisbolsas", "tb_tipo_anexo") }}
    )

select *
from sisbolsas_tb_tipo_anexo
