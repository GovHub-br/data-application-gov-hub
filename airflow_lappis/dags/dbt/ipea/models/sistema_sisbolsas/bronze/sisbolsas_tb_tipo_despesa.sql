{{ config(materialized="table") }}

with
    sisbolsas_tb_tipo_despesa as (
        select
            co_tipo_despesa::text as co_tipo_despesa,
            tp_categoria::text as tp_categoria,
            ds_tipo_despesa::text as ds_tipo_despesa
        from {{ source("sisbolsas", "tb_tipo_despesa") }}
    )

select *
from sisbolsas_tb_tipo_despesa
