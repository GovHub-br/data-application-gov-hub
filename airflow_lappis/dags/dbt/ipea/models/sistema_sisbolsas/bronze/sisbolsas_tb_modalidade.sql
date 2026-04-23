{{ config(materialized="table") }}

with
    sisbolsas_tb_modalidade as (
        select
            co_modalidade::text as co_modalidade,
            ds_modalidade::text as ds_modalidade,
            co_nivel_escolaridade::text as co_nivel_escolaridade,
            st_nivel_escolaridade::text as st_nivel_escolaridade,
            dt_inicio::text as dt_inicio,
            dt_fim::text as dt_fim
        from {{ source("sisbolsas", "tb_modalidade") }}
    )

select *
from sisbolsas_tb_modalidade
