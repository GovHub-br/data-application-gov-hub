{{ config(materialized="table") }}

with
    sisbolsas_tb_lancadespe_anexo as (
        select
            co_lancamento_despesa::text as co_lancamento_despesa,
            co_anexo::text as co_anexo
        from {{ source("sisbolsas", "tb_lancadespe_anexo") }}
    )

select *
from sisbolsas_tb_lancadespe_anexo
